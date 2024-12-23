package src

import (
	"middleware/common"
	"middleware/worker/schema"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Client struct {
	Id         uuid.UUID
	Connection net.Conn
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		Id:         uuid.New(),
		Connection: conn,
	}
}

func (c *Client) Close() {
	c.Connection.Close()
}

func (c *Client) Recv() (string, error) {
	return common.Receive(c.Connection)
}

func (c *Client) Send(message string) error {
	return common.Send(message, c.Connection)
}

func (c *Client) SendId() error {
	for retries := 0; retries < 3; retries++ {
		if err := common.Send(c.Id.String(), c.Connection); err == nil {
			return nil
		} else if retries == 2 {
			log.Errorf("Failed to send ID to client %s after 3 attempts", c.Id)
			return err
		}
	}
	return nil
}

func (c *Client) SendAlive() error {
	return common.DoWithRetry(func() error {
		return c.Send("ALV")
	}, 3, 2)
}

func (c *Client) SendResultsQ1(q *QueryResultStore[*schema.SOCounter], wg *sync.WaitGroup) error {
	log.Infof("Waiting for Q1 results to be ready")
	defer wg.Done()

	for !q.Finished {
		time.Sleep(1 * time.Second)
	}

	log.Infof("Q1 results are ready")

	q.store.Reset()

	scanner, err := q.store.Scanner()
	if err != nil {
		return err
	}

	for scanner.Scan() {
		result := scanner.Text()

		message := common.ClientMessage{Content: result, Type: common.Type_Results_Q1}

		messageSerialized, err := message.SerializeClientMessage()

		if err != nil {
			common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
		}

		c.Send(messageSerialized)
	}

	log.Debug("Finished reading Q1 results file")

	return nil
}

func (c *Client) SendResultsQ2(q *QueryResultStore[*schema.PlayedTime], wg *sync.WaitGroup) error {
	log.Infof("Waiting for Q2 results to be ready")
	defer wg.Done()

	for !q.Finished {
		time.Sleep(1 * time.Second)
	}

	log.Infof("Q2 results are ready")

	q.store.Reset()

	scanner, err := q.store.Scanner()
	if err != nil {
		return err
	}

	for scanner.Scan() {
		result := scanner.Text()

		message := common.ClientMessage{Content: result, Type: common.Type_Results_Q2}

		messageSerialized, err := message.SerializeClientMessage()

		if err != nil {
			common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
		}

		c.Send(messageSerialized)
	}

	log.Debug("Finished reading Q2 results file")

	return nil
}

func (c *Client) SendResultsQ3(q *QueryResultStore[*schema.NamedReviewCounter], wg *sync.WaitGroup) error {
	log.Infof("Waiting for Q3 results to be ready")
	defer wg.Done()

	for !q.Finished {
		time.Sleep(1 * time.Second)
	}

	log.Infof("Q3 results are ready")

	q.store.Reset()

	scanner, err := q.store.Scanner()
	if err != nil {
		return err
	}

	for scanner.Scan() {
		result := scanner.Text()

		message := common.ClientMessage{Content: result, Type: common.Type_Results_Q3}

		messageSerialized, err := message.SerializeClientMessage()

		if err != nil {
			common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
		}

		c.Send(messageSerialized)
	}

	log.Debug("Finished reading Q3 results file")

	return nil
}

func (c *Client) SendResultsQ4(q *QueryResultStore[*schema.NamedReviewCounter], wg *sync.WaitGroup) error {
	log.Infof("Waiting for Q4 results to be ready")
	defer wg.Done()

	for !q.Finished {
		time.Sleep(1 * time.Second)
	}

	log.Infof("Q4 results are ready")

	q.store.Reset()

	scanner, err := q.store.Scanner()
	if err != nil {
		return err
	}

	for scanner.Scan() {
		result := scanner.Text()

		message := common.ClientMessage{Content: result, Type: common.Type_Results_Q4}

		messageSerialized, err := message.SerializeClientMessage()

		if err != nil {
			common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
		}

		c.Send(messageSerialized)
	}

	log.Debug("Finished reading Q4 results file")

	return nil
}

func (c *Client) SendResultsQ5(q *QueryResultStore[*schema.NamedReviewCounter], wg *sync.WaitGroup) error {
	log.Infof("Waiting for Q5 results to be ready")
	defer wg.Done()

	for !q.Finished {
		time.Sleep(1 * time.Second)
	}

	log.Infof("Q5 results are ready")

	q.store.Reset()

	scanner, err := q.store.Scanner()
	if err != nil {
		return err
	}

	for scanner.Scan() {
		result := scanner.Text()

		message := common.ClientMessage{Content: result, Type: common.Type_Results_Q5}

		messageSerialized, err := message.SerializeClientMessage()

		if err != nil {
			common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
		}

		c.Send(messageSerialized)
	}

	log.Debug("Finished reading Q5 results file")

	return nil
}

func (c *Client) SendEndWithResults() error {
	message := common.ClientMessage{Content: c.Id.String(), Type: common.Type_EndWithResults}
	messageSerialized, err := message.SerializeClientMessage()

	if err != nil {
		common.FailOnError(err, "Failed to serialize message") // UNREACHABLE
	}

	c.Send(messageSerialized)

	return nil
}
