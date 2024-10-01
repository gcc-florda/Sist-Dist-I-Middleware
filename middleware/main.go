package main

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func sendToWebServer(data string, endpoint string) {
	// resp, err := http.Post(endpoint, "text/plain", bytes.NewBuffer([]byte(data)))
	// if err != nil {
	// 	log.Fatalf("Failed to send data to webserver: %s", err)
	// }
	// defer resp.Body.Close()
	// fmt.Printf("Data sent to %s\n", endpoint)
}

func consumeFromQueue(queueName string, ch *amqp.Channel, endpoint string) {
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {
		log.Printf("Received from %s [%s]", queueName, d.Body)
		sendToWebServer(string(d.Body), endpoint)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"steam_analyzer", // name
		"direct",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	_, err = ch.QueueDeclare(
		"games_queue", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare games queue")

	_, err = ch.QueueDeclare(
		"reviews_queue", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare reviews queue")

	err = ch.QueueBind(
		"games_queue",    // queue name
		"games",          // routing key
		"steam_analyzer", // exchange
		false,            // no-wait
		nil,
	)
	failOnError(err, "Failed to bind games queue to exchange")

	err = ch.QueueBind(
		"reviews_queue",  // queue name
		"reviews",        // routing key
		"steam_analyzer", // exchange
		false,            // no-wait
		nil,
	)
	failOnError(err, "Failed to bind reviews queue to exchange")

	go consumeFromQueue("games_queue", ch, "http://localhost:8080/games")
	go consumeFromQueue("reviews_queue", ch, "http://localhost:8080/reviews")

	select {}
}
