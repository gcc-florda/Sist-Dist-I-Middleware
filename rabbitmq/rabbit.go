package rabbitmq

import (
	"middleware/common"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Rabbit struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewRabbit() *Rabbit {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")

	return &Rabbit{
		connection: conn,
		channel:    ch,
	}
}

func (r *Rabbit) Close() {
	r.connection.Close()
	r.channel.Close()
}

func (r *Rabbit) NewExchange(name string, exchangeType string) *Exchange {
	ex := Exchange{
		Channel:     r.channel,
		Name:        name,
		Type:        exchangeType,
		Durable:     true,
		AutoDeleted: false,
		Internal:    false,
		NoWait:      false,
		Arguments:   nil,
	}

	ex.Declare()

	return &ex
}

func (r *Rabbit) NewQueue(name string) *Queue {
	q := Queue{
		Channel:      r.channel,
		ExternalName: name,
		Name:         name,
		Durable:      true,
		AutoDeleted:  false,
		Exclusive:    false,
		NoWait:       false,
		Arguments:    nil,
	}

	q.Declare()

	return &q
}
