package controller

import (
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/controller/enums"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const (
	Routing_Broadcast = iota
	Routing_Unicast
)

type HandlerFactory func(job common.JobID) (Handler, EOFValidator, error)

type EOFValidator interface {
	Finish(receivedEOFs map[enums.TokenName]uint) (*EOFMessage, bool)
}

type Protocol interface {
	Unmarshal(rawData []byte) (DataMessage, error)
	Marshal(common.JobID, *common.IdempotencyID, common.Serializable) (common.Serializable, error)
	Route(partitionKey string) (routingKey string)
	Broadcast() (routes []string)
}

type DataMessage interface {
	JobID() common.JobID
	IdemID() *common.IdempotencyID
	IsEOF() bool
	Data() []byte
}

type routing struct {
	Type int
	Key  string
}

type messageToSend struct {
	Routing  routing
	Sequence uint32
	Callback func()
	JobID    common.JobID
	Body     common.Serializable
	Ack      *amqp.Delivery
}

type messageFromQueue struct {
	Delivery amqp.Delivery
	Message  DataMessage
}

type Controller struct {
	name    string
	rcvFrom []*rabbitmq.Queue
	to      []*rabbitmq.Exchange

	protocol Protocol

	txFwd     chan<- *messageToSend
	rxFwd     <-chan *messageToSend
	factory   HandlerFactory
	handlers  map[common.JobID]*HandlerRuntime
	txFinish  chan<- *HandlerRuntime
	rxFinish  <-chan *HandlerRuntime
	runtimeWG sync.WaitGroup
}

func NewController(controllerName string, from []*rabbitmq.Queue, to []*rabbitmq.Exchange, protocol Protocol, handlerF HandlerFactory) *Controller {

	mts := make(chan *messageToSend, 50)
	h := make(chan *HandlerRuntime, 50)

	c := &Controller{
		name:     controllerName,
		rcvFrom:  from,
		to:       to,
		protocol: protocol,
		txFwd:    mts,
		rxFwd:    mts,
		factory:  handlerF,
		handlers: make(map[common.JobID]*HandlerRuntime),
		txFinish: h,
		rxFinish: h,
	}
	// Artificially add one to keep it spinning as long as we don't get a shutdown
	c.runtimeWG.Add(1)
	go func() {
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGTERM)
		<-term
		// Remove the artificial one
		log.Debugf("Received shutdown signal in controller")
		c.runtimeWG.Done()
	}()
	return c
}

func (q *Controller) getHandler(j common.JobID) (*HandlerRuntime, error) {
	v, ok := q.handlers[j]
	if !ok {
		h, eof, err := q.factory(j)
		if err != nil {
			return nil, err
		}
		hr, err := NewHandlerRuntime(
			q.name,
			j,
			h,
			eof,
			q.txFwd,
			q.txFinish,
		)
		if err != nil {
			return nil, err
		}
		q.handlers[j] = hr
		v = hr
		q.runtimeWG.Add(1)
	}
	return v, nil
}

func (q *Controller) markHandlerAsFinished(h *HandlerRuntime) {
	log.Infof("Action: Marking Handler as Finished %s - %s", h.ControllerName, h.JobId)
	close(h.Tx)
	q.handlers[h.JobId].Tx = nil
}

func (q *Controller) publish(routing string, m common.Serializable) {
	for _, ex := range q.to {
		ex.Publish(routing, m)
	}
}

func (q *Controller) broadcast(m common.Serializable) {
	rks := q.protocol.Broadcast()
	for _, k := range rks {
		q.publish(k, m)
	}
}

func (q *Controller) buildIdemId(sequence uint32) *common.IdempotencyID {
	return &common.IdempotencyID{
		Origin:   q.name,
		Sequence: sequence,
	}
}

func (q *Controller) removeInactiveHandlersTask(s *sync.WaitGroup, rxFinish <-chan bool) {
	defer s.Done()
	ids := make([]uuid.UUID, 0)

	removeIds := func() {
		for _, id := range ids {
			delete(q.handlers, id)
		}
		ids = make([]uuid.UUID, 0)
	}

	finishHandler := func(h *HandlerRuntime) {
		log.Infof("Action: Removing Handler from List %s - %s", h.ControllerName, h.JobId)
		if h.Tx != nil {
			close(h.Tx)
		}
		h.Finish()
		q.runtimeWG.Done()
		ids = append(ids, h.JobId)
	}

	d := 5 * time.Minute
	timer := time.NewTimer(d)
	for {
		select {
		case <-rxFinish:
			for id := range q.handlers {
				finishHandler(q.handlers[id])
			}
			removeIds()
			return
		case <-timer.C:
			for id := range q.handlers {
				q.handlers[id].Mark.Add(1)
				if q.handlers[id].Mark.CompareAndSwap(q.handlers[id].Mark.Load(), 2) {
					finishHandler(q.handlers[id])
				}
			}
			removeIds()

			timer.Reset(d)
		}
	}

}

func (q *Controller) finishHandlerTask(s *sync.WaitGroup) {
	defer s.Done()
	for h := range q.rxFinish {
		q.markHandlerAsFinished(h)
	}
	log.Debugf("Closed all handlers")
}

func (q *Controller) closingTask(s *sync.WaitGroup) {
	defer s.Done()
	// Once we got a Shutdown AND all the runtimes closed themselves, then close the controller.
	q.runtimeWG.Wait()
	// At this point, absolutely no handler runtime is running. We can close this safely
	close(q.txFinish)
	close(q.txFwd)
	log.Debugf("Shut down controller")
}

func (q *Controller) sendForwardTask(s *sync.WaitGroup) {
	defer s.Done()
	// Listen for messages to send until all handlers AND a shutdown happened.
	for mts := range q.rxFwd {
		m, err := q.protocol.Marshal(mts.JobID, q.buildIdemId(mts.Sequence), mts.Body)
		if err != nil {
			log.Error(err)
		}

		if mts.Routing.Type == Routing_Broadcast {
			q.broadcast(m)
			if mts.Ack != nil {
				mts.Ack.Ack(false)
			}
		}

		if mts.Routing.Type == Routing_Unicast {
			q.publish(q.protocol.Route(mts.Routing.Key), m)
			if mts.Ack != nil {
				mts.Ack.Ack(false)
			}
		}

		if mts.Callback != nil {
			mts.Callback()
		}
	}

	log.Debugf("Sent all pending messages")
}

func (q *Controller) Start() {
	var end sync.WaitGroup
	f := make(chan bool, 1)
	end.Add(1)
	go q.closingTask(&end)

	end.Add(1)
	go q.finishHandlerTask(&end)

	end.Add(1)
	go q.sendForwardTask(&end)

	end.Add(1)
	go q.removeInactiveHandlersTask(&end, f)

	cases := make([]reflect.SelectCase, len(q.rcvFrom))
	for i, ch := range q.rcvFrom {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.Consume()),
		}
	}

mainloop:
	for {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// At this point, all queues are closed and no messages are in flight
			log.Infof("Closed Queue %s, exiting loop as all Queues are needed.", q.rcvFrom[chosen].ExternalName)
			break mainloop
		}
		d, ok := value.Interface().(amqp.Delivery)
		if !ok {
			log.Fatalf("This really shouldn't happen. How did we got here")
		}

		dm, err := q.protocol.Unmarshal(d.Body)
		if err != nil {
			log.Errorf("Error while parsing the Queue %s message: %s", q.rcvFrom[chosen].ExternalName, err)
			d.Nack(false, true)
			continue
		}

		h, err := q.getHandler(dm.JobID())
		if err != nil {
			log.Errorf("Error while getting a handler for Queue %s, JobID: %s Error: %s", q.rcvFrom[chosen].ExternalName, dm.JobID(), err)
			d.Nack(false, true)
			continue
		}

		if h.Tx != nil {
			h.Tx <- &messageFromQueue{
				Delivery: d,
				Message:  dm,
			}
		} else {
			log.Debugf("A repeated message for an already finished handler was received. AutoACKING")
			// The handler signaled that it finished, this is duplicated
			d.Ack(false)
		}
	}
	log.Debugf("Ending main loop")
	// We have sent everything in flight, finalize the handlers
	f <- true

	end.Wait()
	log.Debugf("Finalized main loop for controller")
}

func (q *Controller) SignalNoMoreMessages() {
	// for k := range q.handlers {
	// 	close(q.handlers[k].forJob)
	// 	q.handlers[k].Finish()
	// }
}
