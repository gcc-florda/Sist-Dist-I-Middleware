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
	name         string
	rcvFrom      []*rabbitmq.Queue
	to           []*rabbitmq.Exchange
	handlerChan  chan *messageToSend
	protocol     Protocol
	factory      HandlerFactory
	handlers     map[common.JobID]*HandlerRuntime
	housekeeping chan *HandlerRuntime
	runtimeWG    sync.WaitGroup
}

func NewController(controllerName string, from []*rabbitmq.Queue, to []*rabbitmq.Exchange, protocol Protocol, handlerF HandlerFactory) *Controller {

	mts := make(chan *messageToSend, 50)
	h := make(chan *HandlerRuntime, 50)

	c := &Controller{
		name:         controllerName,
		rcvFrom:      from,
		to:           to,
		protocol:     protocol,
		handlerChan:  mts,
		factory:      handlerF,
		handlers:     make(map[common.JobID]*HandlerRuntime),
		housekeeping: h,
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
			q.handlerChan,
			q.housekeeping,
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

func (q *Controller) removeHandler(h *HandlerRuntime) {
	// BLOCKING CALL, wait for the handler to finish all its work.
	// Not a problem that this blocks, because the others handlers
	// are working in a separate goroutine, so they can do work.
	log.Infof("Action: Deleting Handler %s - %s", h.name, h.JobId)
	h.Finish()
	delete(q.handlers, h.JobId)
	q.runtimeWG.Done()
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

func (q *Controller) Start() {
	var end sync.WaitGroup

	end.Add(1)
	go func() {
		defer end.Done()
		// Once we got a Shutdown AND all the runtimes closed themselves, then close the controller.
		q.runtimeWG.Wait()
		// At this point, absolutely no handler runtime is running. We can close this safely
		close(q.housekeeping)
		close(q.handlerChan)
		log.Debugf("Shut down controller")
	}()

	end.Add(1)
	go func() {
		defer end.Done()
		for h := range q.housekeeping {
			q.removeHandler(h)
		}
		log.Debugf("Closed all handlers")
	}()

	end.Add(1)
	go func() {
		defer end.Done()
		// Listen for messages to send until all handlers AND a shutdown happened.
		for mts := range q.handlerChan {
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
	}()

	chans := make([]<-chan amqp.Delivery, len(q.rcvFrom))
	for i, v := range q.rcvFrom {
		chans[i] = v.Consume()
	}

	cases := make([]reflect.SelectCase, len(q.rcvFrom))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
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

		h.forJob <- &messageFromQueue{
			Delivery: d,
			Message:  dm,
		}
	}
	log.Debugf("Ending main loop")
	// We have sent everything in flight, finalize the handlers
	q.SignalNoMoreMessages()

	end.Wait()
	log.Debugf("Finalized main loop for controller")
}

func (q *Controller) SignalNoMoreMessages() {
	for k := range q.handlers {
		close(q.handlers[k].forJob)
	}
}
