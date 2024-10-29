package src

import (
	"middleware/common"
	"net"

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
