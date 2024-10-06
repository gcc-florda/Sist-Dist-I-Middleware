package rabbitmq

import (
	"middleware/common/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	Channel     *amqp.Channel
	Context     *Context
	Name        string
	Type        string
	Durable     bool
	AutoDeleted bool
	Internal    bool
	NoWait      bool
	Arguments   []string
}

const (
	ExchangeDirect = "direct"
	ExchangeFanout = "fanout"
	ExchangeTopic  = "topic"
)

func (e *Exchange) Declare() {
	err := e.Channel.ExchangeDeclare(
		e.Name,
		e.Type,
		e.Durable,
		e.AutoDeleted,
		e.Internal,
		e.NoWait,
		nil,
	)
	utils.FailOnError(err, "Failed to declare an exchange")

	e.Context = NewContext()
}

func (e *Exchange) Publish(routingKey string, body []byte) { // TODO: body Serializable
	err := e.Channel.PublishWithContext(e.Context.Context,
		e.Name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body, // TODO: body.Serialize(),
		},
	)
	utils.FailOnError(err, "Failed to publish a message")
}
