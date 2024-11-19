package main

import (
	"net"
)

type WorkerStatus struct {
	Name       string
	Alive      bool
	Connection net.Conn
}

func NewWorkerStatus(name string) *WorkerStatus {
	return &WorkerStatus{
		Name:  name,
		Alive: false,
	}
}
