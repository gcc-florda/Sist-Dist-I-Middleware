package controller

import "middleware/common"

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
		return nil, err
	}
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
	for msg := range h.forJob {
		if !msg.Message.IsEOF() {
			h.handleDataMessage(msg)
		} else {
			eof, err := EOFMessageFromBytes(msg.Message.Data())
			if err != nil {
				msg.Delivery.Nack(false, true)
			}
			h.validateEOF.Update(eof.TokenName)
			msg.Delivery.Ack(false)

			msg, finished := h.validateEOF.Finish(h.eofs.Received)
			if finished {
				ok := h.handleNextStage()
				if ok {
					h.toController <- h.broadcast(msg)
				}
			}

		}
	}
}

func (h *HandlerRuntime) handleDataMessage(msg *messageFromQueue) {
	out, err := h.handler.Handle(msg.Message.Data())
	if err != nil {
		log.Errorf("There was an error while handling a message in JobID: %s. Error: %s", h.jobId, err)
		msg.Delivery.Nack(false, true)
	}
	if out != nil {
		h.toController <- h.unicast(out)
	}
	msg.Delivery.Ack(false)
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
			h.toController <- h.unicast(r)
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

func (h *HandlerRuntime) unicast(m Partitionable) *messageToSend {
	return &messageToSend{
		JobID: h.jobId,
		Routing: routing{
			Type: Routing_Unicast,
			Key:  m.PartitionKey(),
		},
		Body: m,
	}
}

func (h *HandlerRuntime) broadcast(m Partitionable) *messageToSend {
	return &messageToSend{
		JobID: h.jobId,
		Routing: routing{
			Type: Routing_Broadcast,
		},
		Body: m,
	}
}
