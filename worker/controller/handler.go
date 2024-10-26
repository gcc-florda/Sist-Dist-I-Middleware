package controller

import (
	"middleware/common"
	"middleware/worker/schema"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerRuntime struct {
	JobId           common.JobID
	toController    chan *messageToSend
	handler         Handler
	validateEOF     EOFValidator
	forJob          chan *messageFromQueue
	housekeeping    chan *HandlerRuntime
	eofs            *EOFState
	removeOnCleanup bool
	finish          chan bool
}

func NewHandlerRuntime(j common.JobID, handler Handler, validator EOFValidator, send chan *messageToSend, housekeeping chan *HandlerRuntime) (*HandlerRuntime, error) {
	eof, err := NewEOFState(common.Config.GetString("metasavepath"), j.String())
	if err != nil {
		return nil, err
	}
	c := &HandlerRuntime{
		JobId:           j,
		handler:         handler,
		validateEOF:     validator,
		toController:    send,
		forJob:          make(chan *messageFromQueue, 50),
		housekeeping:    housekeeping,
		eofs:            eof,
		removeOnCleanup: false,
		finish:          make(chan bool, 1),
	}

	go c.Start()
	return c, nil
}

func (h *HandlerRuntime) Start() {
	// We get here if and only if
	//	- We finalize the job for this handler
	//	- An external force closed the channel for receiving messages
	defer h.cleanup()
	defer func() {
		h.finish <- true
	}()

	msg, finished := h.validateEOF.Finish(h.eofs.Received)
	if finished {
		ok := h.handleNextStage()
		if ok {
			h.toController <- h.broadcast(msg)
			h.removeOnCleanup = true
			return
		}
	}

	for msg := range h.forJob {
		if !msg.Message.IsEOF() {
			h.handleDataMessage(msg)
		} else {
			eof, err := EOFMessageFromBytes(msg.Message.Data())
			if err != nil {
				log.Debugf("Esploto cuando llego el EOF porque no se parseo %s", err)
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
				h.removeOnCleanup = true
				return
			}
		}
	}

	log.Debugf("Finalized runtime for handler for %s", h.JobId.String())
}

func (h *HandlerRuntime) Finish() {
	// Ensure that the runtime has sent everything to the controller
	<-h.finish
	h.handler.Shutdown(h.removeOnCleanup)
}

func (h *HandlerRuntime) handleDataMessage(msg *messageFromQueue) {
	out, err := h.handler.Handle(msg.Message.Data())
	if err != nil {
		log.Errorf("There was an error while handling a message in JobID: %s. Error: %s", h.JobId, err)
		msg.Delivery.Nack(false, true)
	}
	if out != nil {
		h.toController <- h.unicast(out, &msg.Delivery)
	} else {
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
			log.Errorf("There was an error while handling a next stage message in JobID: %s. Error: %s", h.JobId, err)
			return false
		}
	}
	return true
}

func (h *HandlerRuntime) unicast(m schema.Partitionable, d *amqp.Delivery) *messageToSend {
	return &messageToSend{
		JobID: h.JobId,
		Routing: routing{
			Type: Routing_Unicast,
			Key:  m.PartitionKey(),
		},
		Body: m,
		Ack:  d,
	}
}

func (h *HandlerRuntime) broadcast(m schema.Partitionable) *messageToSend {
	return &messageToSend{
		JobID: h.JobId,
		Routing: routing{
			Type: Routing_Broadcast,
		},
		Body: m,
		Ack:  nil,
	}
}

func (h *HandlerRuntime) cleanup() {
	h.housekeeping <- h
}
