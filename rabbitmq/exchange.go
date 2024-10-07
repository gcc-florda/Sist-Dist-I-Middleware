package rabbitmq

import (
	"middleware/common"

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
	common.FailOnError(err, "Failed to declare an exchange")

	e.Context = NewContext()
}

func (e *Exchange) Publish(routingKey string, body common.Serializable) {
	err := e.Channel.PublishWithContext(e.Context.Context,
		e.Name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body.Serialize(),
		},
	)
	common.FailOnError(err, "Failed to publish a message")

	log.Debugf("Published message to exchange %s with routing key %s", e.Name, routingKey)
}
