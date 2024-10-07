package controller

import (
	"middleware/common"
	"middleware/rabbitmq"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type JobID = string

type HandlerFactory func(job JobID) (Handler, error)

type Handler interface {
	Handle(protocolData []byte) error
	NextStage() (<-chan *common.Serializable, <-chan error)
	Shutdown()
}

type Protocol interface {
	Unmarshal(rawData []byte) (DataMessage, error)
	Marshal(any) ([]byte, error)
}

type DataMessage interface {
	JobID() JobID
	Data() []byte
}

type Controller struct {
	from        *rabbitmq.Queue
	consumeFrom <-chan amqp.Delivery
	to          *rabbitmq.Exchange
	protocol    Protocol
	factory     HandlerFactory
	handlers    map[JobID]Handler
}

func NewController(from *rabbitmq.Queue, to *rabbitmq.Exchange, protocol Protocol, handlerF HandlerFactory) *Controller {

	return &Controller{
		from:        from,
		consumeFrom: from.Consume(),
		to:          to,
		protocol:    protocol,
		factory:     handlerF,
		handlers:    make(map[JobID]Handler),
	}
}

func (q *Controller) getHandler(j JobID) (Handler, error) {
	v, ok := q.handlers[j]
	if !ok {
		v, err := q.factory(j)
		if err != nil {
			return nil, err
		}
		q.handlers[j] = v
	}
	return v, nil
}

func (q *Controller) Start() {
	for message := range q.consumeFrom {
		parsed, err := q.protocol.Unmarshal(message.Body)
		if err != nil {
			log.Errorf("Error while parsing the Queue %s message: %s", q.from.ExternalName, err)
			message.Nack(false, true)
			continue
		}
		h, err := q.getHandler(parsed.JobID())

		if err != nil {
			log.Errorf("Error while creating the Handler for Queue %s: %s", q.from.ExternalName, err)
			message.Nack(false, true)
			continue
		}

		err = h.Handle(parsed.Data())

		if err != nil {
			log.Errorf("Error while handling the message from Queue %s: %s", q.from.ExternalName, err)
			message.Nack(false, true)
			continue
		}

		message.Ack(false)
	}
}
