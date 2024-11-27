package controller

import (
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"

	amqp "github.com/rabbitmq/amqp091-go"
)

type NextStageMessage struct {
	Message      schema.Partitionable
	Sequence     uint32
	SentCallback func()
}

type Handler interface {
	Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*NextStageMessage, error)
	NextStage() (<-chan *NextStageMessage, <-chan error)
	Shutdown(delete bool)
}

type HandlerRuntime struct {
	JobId           common.JobID
	name            string
	toController    chan *messageToSend
	handler         Handler
	validateEOF     EOFValidator
	forJob          chan *messageFromQueue
	housekeeping    chan *HandlerRuntime
	eofs            *EOFState
	removeOnCleanup bool
	finish          chan bool
	sequenceCounter uint32
}

func NewHandlerRuntime(controllerName string, j common.JobID, handler Handler, validator EOFValidator, send chan *messageToSend, housekeeping chan *HandlerRuntime) (*HandlerRuntime, error) {
	eof, err := NewEOFState(common.Config.GetString("metasavepath"), filepath.Join(controllerName, j.String()))
	if err != nil {
		return nil, err
	}
	c := &HandlerRuntime{
		JobId:           j,
		name:            controllerName,
		handler:         handler,
		validateEOF:     validator,
		toController:    send,
		forJob:          make(chan *messageFromQueue, 10000),
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
	defer func() {
		log.Infof("Action: Handler Runtime Finalizing %s - %s", h.name, h.JobId)
		h.finish <- true
		close(h.finish)
	}()

	for msg := range h.forJob {
		if !msg.Message.IsEOF() {
			h.handleDataMessage(msg)
			continue
		}

		eof, err := EOFMessageFromBytes(msg.Message.Data())
		if err != nil {
			msg.Delivery.Nack(false, true)
		}
		updated := h.eofs.Update(eof.TokenName, msg.Message.IdemID())

		msgFwd, finished := h.validateEOF.Finish(h.eofs.Received)
		if updated && finished {
			ok := h.handleNextStage()
			if ok {
				h.sequenceCounter += 1
				// Ack the EOF that generated the send forward to
				h.toController <- h.broadcast(&NextStageMessage{
					Message:  msgFwd,
					Sequence: h.sequenceCounter,
					SentCallback: func() {
						// This guarantees that the only one that can trigger the EOF forward
						// sequence is the last one that has arrived, if there are others that are
						// repeated, they will not generate the sequence, as they will never update
						// and we will missing a token to start the sequence
						h.eofs.SaveState(eof.TokenName, msg.Message.IdemID())
						msg.Delivery.Ack(false)
						h.removeOnCleanup = true
						h.signalFinish()
					},
				}, nil)
			}
		} else if updated {
			h.eofs.SaveState(eof.TokenName, msg.Message.IdemID())
			msg.Delivery.Ack(false)
		} else if !updated && finished {
			h.signalFinish()
			msg.Delivery.Ack(false)
		} else {
			msg.Delivery.Ack(false)
		}
	}
}

func (h *HandlerRuntime) signalFinish() {
	// Tell the controller to close my sending channel
	h.housekeeping <- h
}

func (h *HandlerRuntime) Finish() {
	// Ensure that the runtime has sent everything to the controller
	log.Debugf("Action: Received Finishing Signal to Finish %s-%s", h.name, h.JobId)
	<-h.finish
	log.Debugf("Action: Shutting Down %s-%s | Remove: %t", h.name, h.JobId, h.removeOnCleanup)
	h.handler.Shutdown(h.removeOnCleanup)
	log.Debugf("Action: Shutdown %s-%s", h.name, h.JobId)
}

func (h *HandlerRuntime) handleDataMessage(msg *messageFromQueue) {
	out, err := h.handler.Handle(msg.Message.Data(), msg.Message.IdemID())
	if err != nil {
		log.Errorf("Action: Handling Message %s - %s| Result: Error | Error: %s | Data: %s", h.name, h.JobId, err, msg.Message.Data())
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
			if err == nil && !ok {
				continue
			}
			log.Errorf("Action: Next Stage Message %s - %s | Results: Error | Error: %s", h.name, h.JobId, err)
			return false
		}
	}
	return true
}

func (h *HandlerRuntime) unicast(m *NextStageMessage, d *amqp.Delivery) *messageToSend {
	h.sequenceCounter = m.Sequence
	return &messageToSend{
		JobID:    h.JobId,
		Sequence: m.Sequence,
		Callback: m.SentCallback,
		Body:     m.Message,
		Ack:      d,
		Routing: routing{
			Type: Routing_Unicast,
			Key:  m.Message.PartitionKey(),
		},
	}
}

func (h *HandlerRuntime) broadcast(m *NextStageMessage, d *amqp.Delivery) *messageToSend {
	return &messageToSend{
		JobID:    h.JobId,
		Sequence: m.Sequence,
		Callback: m.SentCallback,
		Routing: routing{
			Type: Routing_Broadcast,
		},
		Body: m.Message,
		Ack:  d,
	}
}
