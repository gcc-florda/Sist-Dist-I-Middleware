package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func sendToExchange(exchangeName, routingKey, data string, ch *amqp.Channel) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf("Sent [%s]", data)
}

func readAndSendData(filePath string, routingKey string, exchangeName string, ch *amqp.Channel, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	failOnError(err, "Failed to open file: "+filePath)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		sendToExchange(exchangeName, routingKey, scanner.Text(), ch)
	}
	if err := scanner.Err(); err != nil {
		failOnError(err, "Error reading file: "+filePath)
	}

	// log.Printf("Data from %s sent to RabbitMQ\n", filePath)
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

	var wg sync.WaitGroup
	wg.Add(2)

	go readAndSendData("/app/datasets/games.csv", "games", "steam_analyzer", ch, &wg)
	go readAndSendData("/app/datasets/reviews.csv", "reviews", "steam_analyzer", ch, &wg)

	wg.Wait()

	log.Println("All data sent successfully!")
}
