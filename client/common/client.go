package common

import (
	"bufio"
	"fmt"
	"io"
	"middleware/common/utils"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

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
	log.Criticalf("Received SIGTERM")
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) CreateSocket() {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	utils.FailOnError(err, "Failed to connect to server")
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

	utils.Send("END\n", c.conn)
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
	utils.FailOnError(err, fmt.Sprintf("Failed to open file %s", path))

	defer file.Close()

	reader := bufio.NewReader(file)

	err = c.SendBatches(reader)
	utils.FailOnError(err, fmt.Sprintf("Failed to send data from file %s", path))
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

	utils.Send(message, c.conn)
}
