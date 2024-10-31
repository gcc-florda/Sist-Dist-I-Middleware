package src

import (
	"bufio"
	"fmt"
	"io"
	"middleware/common"
	"path/filepath"

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
	Id         string
	Config     ClientConfig
	Connection net.Conn
	Term       chan os.Signal
	Results    map[int]*common.TemporaryStorage
}

func assertNoErrTemp(p string) *common.TemporaryStorage {
	s, err := common.NewTemporaryStorage(p)
	if err != nil {
		panic("Not a problem for now")
	}
	return s
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		Config: config,
		Term:   make(chan os.Signal, 1),
		Results: map[int]*common.TemporaryStorage{
			common.Type_Results_Q1: assertNoErrTemp(filepath.Join(".", "results", "query_one.csv")),
			common.Type_Results_Q2: assertNoErrTemp(filepath.Join(".", "results", "query_two.csv")),
			common.Type_Results_Q3: assertNoErrTemp(filepath.Join(".", "results", "query_three.csv")),
			common.Type_Results_Q4: assertNoErrTemp(filepath.Join(".", "results", "query_four.csv")),
			common.Type_Results_Q5: assertNoErrTemp(filepath.Join(".", "results", "query_five.csv")),
		},
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

	err := c.GetId()

	if err != nil {
		log.Criticalf("Failed to get client ID")
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go c.SendData(c.Config.GamesFilePath, &wg)

	go c.SendData(c.Config.ReviewsFilePath, &wg)

	wg.Wait()

	c.AskForResults()

	c.CloseConnection()

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
	common.FailOnError(err, fmt.Sprintf("Failed to open file %s", path))

	defer file.Close()

	var messageType int

	if strings.Contains(path, "games") {
		messageType = common.Type_GAMES
	} else if strings.Contains(path, "reviews") {
		messageType = common.Type_REVIEWS
	}

	reader := bufio.NewReader(file)

	err = c.SendBatches(reader, messageType)
	common.FailOnError(err, fmt.Sprintf("Failed to send data from file %s", path))
}

func (c *Client) SendBatches(reader *bufio.Reader, messageType int) error {
	lastBatch := common.NewBatch()

	// Ignore header
	_, err := reader.ReadString('\n')

	if err == io.EOF {
		log.Criticalf("The file is empty")
		return err
	}

	i := 0
	for {
		line, err := reader.ReadString('\n')

		var content string

		if err == io.EOF {
			content = common.EOF
		} else if err != nil {
			return err
		} else {
			content = line
		}

		clientMessage := common.ClientMessage{Content: content, Type: messageType}
		clientMessageSerialized, err := clientMessage.SerializeClientMessage()

		common.FailOnError(err, "Failed to serialize message") // UNREACHABLE

		if clientMessage.IsEOF() {
			c.SendBatch(lastBatch)
			common.Send(clientMessageSerialized, c.Connection) // EOF
			log.Debugf("Seding EOF: %s", clientMessageSerialized)
			break
		}

		if !lastBatch.CanHandle(clientMessageSerialized, c.Config.BatchMaxAmount) {
			c.SendBatch(lastBatch)

			lastBatch = common.NewBatch()
		}

		lastBatch.AppendData(clientMessageSerialized)
		i++
	}

	log.Debugf("Stopped Client loop %d", i)
	return nil
}

func (c *Client) SendBatch(batch common.Batch) {
	if batch.Size() == 0 {
		return
	}

	message := batch.Serialize()

	common.Send(message, c.Connection)
}

func (c *Client) GetId() error {
	const maxRetries = 3
	var err error

	for i := 0; i < maxRetries; i++ {
		c.Id, err = common.Receive(c.Connection)
		if err == nil {
			log.Infof("Received client ID: %s", c.Id)
			return nil
		}

		log.Criticalf("Failed to receive client ID: %v", i+1)
		time.Sleep(5 * time.Second)
	}

	return err
}

func (c *Client) AskForResults() {
	defer func() {
		for key := range c.Results {
			c.Results[key].Close()
		}
	}()
	// ASKING FOR RESULTS
	log.Debug("Asking for results")
	clientMessage := common.ClientMessage{Content: c.Id, Type: common.Type_AskForResults}
	clientMessageSerialized, err := clientMessage.SerializeClientMessage()

	common.FailOnError(err, "Failed to serialize message") // UNREACHABLE

	common.Send(clientMessageSerialized, c.Connection)

	log.Debugf("Asked for results: %s", clientMessageSerialized)

	// GETTING RESULTS
	for {
		message, err := common.Receive(c.Connection)

		if err != nil {
			log.Errorf("Connection with Server has been closed")
			return
		}

		messageDeserialized, err := common.DeserializeClientMessage(message)

		common.FailOnError(err, "Failed to deserialize message") // UNREACHABLE

		if messageDeserialized.IsEndWithResults() {
			log.Infof("Finish reading results for all queries")
			return
		} else if messageDeserialized.IsQueryResult() {
			writeTo, ok := c.Results[messageDeserialized.Type]
			if !ok {
				log.Errorf("Action: Rerceived Query Result | Result: No place to store | Data: %s", message)
				continue
			}
			writeTo.AppendLine([]byte(messageDeserialized.Content))
		} else {
			log.Errorf("Unexpected message from server")
			return
		}
	}
}

func (c *Client) CloseConnection() {
	clientMessage := common.ClientMessage{Content: c.Id, Type: common.Type_CloseConnection}
	clientMessageSerialized, err := clientMessage.SerializeClientMessage()

	common.FailOnError(err, "Failed to serialize message") // UNREACHABLE

	common.Send(clientMessageSerialized, c.Connection)
}
