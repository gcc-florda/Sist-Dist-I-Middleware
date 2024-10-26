package src

import (
	"bufio"
	"fmt"
	"io"
	"middleware/common"

	"net"
	"os"
	"os/signal"
	"strings"
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
	Config     ClientConfig
	Connection net.Conn
	Term       chan os.Signal
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		Config: config,
		Term:   make(chan os.Signal, 1),
	}

	signal.Notify(client.Term, syscall.SIGTERM)

	return client
}

func (c *Client) HandleShutdown() {
	<-c.Term
	log.Criticalf("Received SIGTERM")
	if c.Connection != nil {
		c.Connection.Close()
	}
}

func (c *Client) CreateSocket() {
	time.Sleep(5 * time.Second)
	conn, err := net.Dial("tcp", c.Config.ServerAddress)
	common.FailOnError(err, "Failed to connect to server")
	c.Connection = conn
}

func (c *Client) StartClient() {
	go c.HandleShutdown()

	c.CreateSocket()
	log.Infof("Connected to server at %s", c.Config.ServerAddress)

	var wg sync.WaitGroup
	wg.Add(2)

	go c.SendData(c.Config.GamesFilePath, &wg)

	go c.SendData(c.Config.ReviewsFilePath, &wg)

	wg.Wait()

	log.Infof("All data sent to server. Exiting")

	common.Send(common.END, c.Connection)
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
	common.FailOnError(err, fmt.Sprintf("Failed to open file %s", path))

	defer file.Close()

	var pathType string

	if strings.Contains(path, "games") {
		pathType = "1"
	} else if strings.Contains(path, "reviews") {
		pathType = "2"
	}

	reader := bufio.NewReader(file)

	err = c.SendBatches(reader, pathType)
	common.FailOnError(err, fmt.Sprintf("Failed to send data from file %s", path))
}

func (c *Client) SendBatches(reader *bufio.Reader, pathType string) error {
	lastBatch := common.NewBatch()

	// Ignore header
	_, err := reader.ReadString('\n')

	if err == io.EOF {
		log.Criticalf("The file is empty")
		return nil
	}

	common.FailOnError(err, "Failed to read header from file")

	for {
		line, err := reader.ReadString('\n')

		line = pathType + "," + line

		if err == io.EOF {
			c.SendBatch(lastBatch)
			common.Send(pathType+",EOF", c.Connection)
			time.Sleep(c.Config.BatchSleep)
			break
		}

		if err != nil {
			return err
		}

		if !lastBatch.CanHandle(line, c.Config.BatchMaxAmount) {
			c.SendBatch(lastBatch)
			time.Sleep(c.Config.BatchSleep)

			lastBatch = common.NewBatch()
		}

		lastBatch.AppendData(line)
	}

	return nil
}

func (c *Client) SendBatch(batch common.Batch) {
	if batch.Size() == 0 {
		return
	}

	message := batch.Serialize()

	common.Send(message, c.Connection)
}
