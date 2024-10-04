package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"middleware/utils"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ClientConfig struct {
	ServerAddress   string
	BatchMaxAmount  int
	BatchSleep      time.Duration
	GamesFilePath   string
	ReviewsFilePath string
}

type Client struct {
	config ClientConfig
	conn   net.Conn
	term   chan os.Signal
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
		term:   make(chan os.Signal, 1),
	}

	signal.Notify(client.term, syscall.SIGTERM)

	return client
}

func (c *Client) HandleShutdown() {
	<-c.term
	log.Criticalf("Received SIGTERM, shutting down")
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) CreateSocket() {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	FailOnError(err, "Failed to connect to server")
	c.conn = conn
}

func (c *Client) StartClient() {
	go c.HandleShutdown()

	c.CreateSocket()
	log.Infof("Connected to server at %s", c.config.ServerAddress)

	var wg sync.WaitGroup
	wg.Add(2)

	go c.SendData(c.config.GamesFilePath, &wg)

	go c.SendData(c.config.ReviewsFilePath, &wg)

	wg.Wait()

	log.Infof("All data sent to server. Exiting")
}

func (c *Client) OpenFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (c *Client) SendData(path string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := c.OpenFile(path)
	FailOnError(err, fmt.Sprintf("Failed to open file %s", path))

	defer file.Close()

	reader := bufio.NewReader(file)

	err = c.SendBatches(reader)
	FailOnError(err, fmt.Sprintf("Failed to send data from file %s", path))
}

func (c *Client) SendBatches(reader *bufio.Reader) error {
	lastBatch := utils.NewBatch()

	for {
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			c.SendBatch(lastBatch)
			time.Sleep(c.config.BatchSleep)
			break
		}

		if err != nil {
			return err
		}

		if !lastBatch.CanHandle(line, c.config.BatchMaxAmount) {
			c.SendBatch(lastBatch)
			time.Sleep(c.config.BatchSleep)

			lastBatch = utils.NewBatch()
		}

		lastBatch.AppendData(line)
	}

	return nil
}

func (c *Client) SendBatch(batch utils.Batch) {
	if batch.Size() == 0 {
		return
	}

	message := batch.Serialize()

	c.Send(message)
}

func (c *Client) Send(message string) {
	messageBytes := []byte(message)

	buffer := new(bytes.Buffer)

	err := binary.Write(buffer, binary.BigEndian, uint32(len(messageBytes)))
	FailOnError(err, "Failed to write message length to buffer")

	err = binary.Write(buffer, binary.BigEndian, messageBytes)
	FailOnError(err, "Failed to write message to buffer")

	messageLength := buffer.Len()
	bytesSent := 0

	for bytesSent < messageLength {
		n, err := c.conn.Write(buffer.Bytes())
		FailOnError(err, "Failed to send bytes to server")
		bytesSent += n
	}
}
