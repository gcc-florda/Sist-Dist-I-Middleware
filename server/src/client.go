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

func (c *Client) Recv() string {
	return common.Receive(c.Connection)
}
