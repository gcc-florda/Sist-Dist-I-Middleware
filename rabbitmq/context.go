package rabbitmq

import (
	"context"
	"time"
)

type Context struct {
	Context context.Context
	Cancel  context.CancelFunc
}

func NewContext() *Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	return &Context{
		Context: ctx,
		Cancel:  cancel,
	}
}

func (c *Context) Close() {
	c.Cancel()
}
