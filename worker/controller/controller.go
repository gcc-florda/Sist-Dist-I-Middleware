package controller

import (
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/controller/enums"
	"reflect"

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
type Handler interface {
	Handle(protocolData []byte) (Partitionable, error)
	NextStage() (<-chan Partitionable, <-chan error)
	Shutdown()
}

type Partitionable interface {
	common.Serializable
	PartitionKey() string
}

type Protocol interface {
	Unmarshal(rawData []byte) (DataMessage, error)
	Marshal(common.JobID, common.Serializable) (common.Serializable, error)
	Route(partitionKey string) (routingKey string)
	Broadcast() (routes []string)
}

type DataMessage interface {
	JobID() common.JobID
	IsEOF() bool
	Data() []byte
}

type routing struct {
	Type int
	Key  string
}

type messageToSend struct {
	Routing routing
	JobID   common.JobID
	Body    common.Serializable
	Ack     *amqp.Delivery
}

type messageFromQueue struct {
	Delivery amqp.Delivery
	Message  DataMessage
}

type Controller struct {
	rcvFrom      []*rabbitmq.Queue
	to           []*rabbitmq.Exchange
	handlerChan  chan *messageToSend
	protocol     Protocol
	factory      HandlerFactory
	handlers     map[common.JobID]*HandlerRuntime
	housekeeping chan common.JobID
}

func NewController(from []*rabbitmq.Queue, to []*rabbitmq.Exchange, protocol Protocol, handlerF HandlerFactory) *Controller {

	mts := make(chan *messageToSend, common.Config.GetInt("controllers.bufferSize.toSend"))
	h := make(chan common.JobID, common.Config.GetInt("controllers.bufferSize.deadHandlers"))

	return &Controller{
		rcvFrom:      from,
		to:           to,
		protocol:     protocol,
		handlerChan:  mts,
		factory:      handlerF,
		handlers:     make(map[common.JobID]*HandlerRuntime),
		housekeeping: h,
	}
}

func (q *Controller) getHandler(j common.JobID) (*HandlerRuntime, error) {
	v, ok := q.handlers[j]
	if !ok {
		v, eof, err := q.factory(j)
		if err != nil {
			return nil, err
		}
		hr, err := NewHandlerRuntime(
			j,
			v,
			eof,
			q.handlerChan,
			q.housekeeping,
		)
		if err != nil {
			return nil, err
		}
		q.handlers[j] = hr
	}
	return v, nil
}

func (q *Controller) removeHandler(j common.JobID) {
	delete(q.handlers, j)
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

func (q *Controller) Start() {
	go func() {
		for h := range q.housekeeping {
			q.removeHandler(h)
		}
	}()

	go func() {
		for mts := range q.handlerChan {
			m, err := q.protocol.Marshal(mts.JobID, mts.Body)
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
		}
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
			log.Errorf("Closed Queue %s, exiting loop as all Queues are needed.", q.rcvFrom[chosen].ExternalName)
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
			log.Errorf("Error while gettin a handler for Queue %s, JobID: %s Error: %s", q.rcvFrom[chosen].ExternalName, dm.JobID(), err)
			d.Nack(false, true)
			continue
		}

		h.forJob <- &messageFromQueue{
			Delivery: d,
			Message:  dm,
		}
	}
}
