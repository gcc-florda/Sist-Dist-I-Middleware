package rabbitmq

import (
	"middleware/common/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	Channel      *amqp.Channel
	ExternalName string
	Name         string
	Durable      bool
	AutoDeleted  bool
	Exclusive    bool
	NoWait       bool
	Arguments    []string
}

func (q *Queue) Declare() {
	queue, err := q.Channel.QueueDeclare(
		q.ExternalName,
		q.Durable,
		q.AutoDeleted,
		q.Exclusive,
		q.NoWait,
		nil,
	)
	utils.FailOnError(err, "Failed to declare a queue")

	q.Name = queue.Name
}

func (q *Queue) Bind(exchange *Exchange, routingKey string) {
	err := q.Channel.QueueBind(
		q.Name,
		routingKey,
		exchange.Name,
		false,
		nil,
	)
	utils.FailOnError(err, "Failed to bind a queue")
}

func (q *Queue) Consume() <-chan amqp.Delivery {
	messages, err := q.Channel.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	utils.FailOnError(err, "Failed to consume messages")

	return messages
}
