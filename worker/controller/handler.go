package controller

import (
	"middleware/common"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerRuntime struct {
	jobId        common.JobID
	toController chan *messageToSend
	handler      Handler
	validateEOF  EOFValidator
	forJob       chan *messageFromQueue
	housekeeping chan common.JobID
	eofs         *EOFState
}

func NewHandlerRuntime(j common.JobID, handler Handler, validator EOFValidator, send chan *messageToSend, housekeeping chan common.JobID) (*HandlerRuntime, error) {
	eof, err := NewEOFState(common.Config.GetString("storage.path"), j.String())
	if err != nil {
		log.Debug("Couldnt create NewEOFState")
		return nil, err
	}

	log.Debugf("Creating HandlerRuntime for JobID: %s", j)

	c := &HandlerRuntime{
		jobId:        j,
		handler:      handler,
		validateEOF:  validator,
		toController: send,
		forJob:       make(chan *messageFromQueue, 50),
		housekeeping: housekeeping,
		eofs:         eof,
	}

	go c.Start()

	return c, nil
}

func (h *HandlerRuntime) Start() {
	log.Debugf("Starting HandlerRuntime for JobID: %s", h.jobId)

	for msg := range h.forJob {
		log.Debugf("Receive msg from forJob chan")
		if !msg.Message.IsEOF() {
			log.Debugf("Message is not EOF --> %v", msg)
			h.handleDataMessage(msg)
		} else {
			log.Debug("Message is EOF")
			eof, err := EOFMessageFromBytes(msg.Message.Data())
			if err != nil {
				msg.Delivery.Nack(false, true)
			}
			h.eofs.Update(eof.TokenName)
			msg.Delivery.Ack(false)
		}

		msg, finished := h.validateEOF.Finish(h.eofs.Received)
		if finished {
			ok := h.handleNextStage()
			if ok {
				h.toController <- h.broadcast(msg)
			}
		}
	}
}

func (h *HandlerRuntime) handleDataMessage(msg *messageFromQueue) {
	log.Debug("Handling message")
	out, err := h.handler.Handle(msg.Message.Data())
	if err != nil {
		log.Errorf("There was an error while handling a message in JobID: %s. Error: %s", h.jobId, err)
		msg.Delivery.Nack(false, true)
	}
	log.Debug("Message handled")
	if out != nil {
		log.Debug("Send message through controller channel")
		h.toController <- h.unicast(out, &msg.Delivery)
	} else {
		log.Debug("Delivery ACK")
		msg.Delivery.Ack(false)
	}
}

func (h *HandlerRuntime) handleNextStage() bool {
	cr, ce := h.handler.NextStage()

sendLoop:
	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break sendLoop
			}
			h.toController <- h.unicast(r, nil)
		case err, ok := <-ce:
			if !ok {
				break sendLoop
			}
			log.Errorf("There was an error while handling a next stage message in JobID: %s. Error: %s", h.jobId, err)
			return false
		}
	}
	return true
}

func (h *HandlerRuntime) unicast(m Partitionable, d *amqp.Delivery) *messageToSend {
	return &messageToSend{
		JobID: h.jobId,
		Routing: routing{
			Type: Routing_Unicast,
			Key:  m.PartitionKey(),
		},
		Body: m,
		Ack:  d,
	}
}

func (h *HandlerRuntime) broadcast(m Partitionable) *messageToSend {
	return &messageToSend{
		JobID: h.jobId,
		Routing: routing{
			Type: Routing_Broadcast,
		},
		Body: m,
		Ack:  nil,
	}
}
