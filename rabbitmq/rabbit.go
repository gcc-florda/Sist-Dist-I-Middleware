package rabbitmq

import (
	"middleware/common"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type Rabbit struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Exchanges  []Exchange
	Queues     []Queue
}

func NewRabbit() *Rabbit {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")

	log.Debugf("Connected to RabbitMQ")
	return &Rabbit{
		Connection: conn,
		Channel:    ch,
	}
}

func (r *Rabbit) Close() {
	r.Connection.Close()
	r.Channel.Close()

	for _, ex := range r.Exchanges {
		ex.Close()
	}
}

func (r *Rabbit) NewExchange(name string, exchangeType string) *Exchange {
	ex := Exchange{
		Channel:     r.Channel,
		Name:        name,
		Type:        exchangeType,
		Durable:     true,
		AutoDeleted: false,
		Internal:    false,
		NoWait:      false,
		Arguments:   nil,
	}

	ex.Declare()

	r.Exchanges = append(r.Exchanges, ex)

	log.Debugf("Declared exchange %s", name)

	return &ex
}

func (r *Rabbit) NewQueue(name string) *Queue {
	q := Queue{
		Channel:      r.Channel,
		ExternalName: name,
		Name:         name,
		Durable:      true,
		AutoDeleted:  false,
		Exclusive:    false,
		NoWait:       false,
		Arguments:    nil,
	}

	q.Declare()

	r.Queues = append(r.Queues, q)

	log.Debugf("Declared queue %s", name)

	return &q
}

func (r *Rabbit) Publish(exchangeName string, routingKey string, body common.Serializable) {
	ex := r.GetExchange(exchangeName)

	if ex == nil {
		log.Panicf("Exchange %s not found", exchangeName)
		return
	}

	ex.Publish(routingKey, body)
}

func (r *Rabbit) GetExchange(name string) *Exchange {
	for _, ex := range r.Exchanges {
		if ex.Name == name {
			return &ex
		}
	}

	return nil
}
