package controller

import (
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/exp/rand"
)

const testing bool = false

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
	JobId          common.JobID
	Tx             chan<- *messageFromQueue
	ControllerName string
	Mark           int32

	txFwd chan<- *messageToSend

	handler     Handler
	validateEOF EOFValidator
	rx          <-chan *messageFromQueue
	eofs        *EOFState

	removeOnCleanup bool
	finish          chan bool
	sequenceCounter uint32

	r *rand.Rand
}

func NewHandlerRuntime(
	controllerName string,
	j common.JobID,
	handler Handler,
	validator EOFValidator,
	send chan<- *messageToSend,
) (*HandlerRuntime, error) {
	eof, err := NewEOFState(common.Config.GetString("metasavepath"), filepath.Join(controllerName, j.String()))
	if err != nil {
		return nil, err
	}

	r := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	ch := make(chan *messageFromQueue, 100)

	c := &HandlerRuntime{
		JobId:           j,
		Tx:              ch,
		ControllerName:  controllerName,
		handler:         handler,
		validateEOF:     validator,
		txFwd:           send,
		rx:              ch,
		eofs:            eof,
		removeOnCleanup: false,
		finish:          make(chan bool, 1),
		Mark:            0,
		r:               r,
	}

	go c.Start()
	return c, nil
}

func (h *HandlerRuntime) randAmount() int {
	weights := []float64{0.9, 0.07, 0.03} // High bias for 1, less for 2, much less for 3
	r := h.r.Float64()
	accum := 0.0
	for i, weight := range weights {
		accum += weight
		if r < accum {
			return i // Return the corresponding number (0,1,2)
		}
	}
	return 1
}

func (h *HandlerRuntime) sendForward(m *messageToSend) {
	if testing {
		cpy := &messageToSend{
			Routing:  m.Routing,
			Sequence: m.Sequence,
			JobID:    m.JobID,
			Body:     m.Body,
			Callback: nil,
			Ack:      nil,
		}
		h.txFwd <- m
		for i := 0; i < h.randAmount(); i++ {
			h.txFwd <- cpy
		}
		return
	}
	h.txFwd <- m
}

func (h *HandlerRuntime) Start() {
	// We get here if and only if
	//	- We finalize the job for this handler
	//	- An external force closed the channel for receiving messages
	defer func() {
		log.Infof("Action: Handler Runtime Finalizing %s - %s", h.ControllerName, h.JobId)
		h.finish <- true
		close(h.finish)
	}()

	for msg := range h.rx {
		// We are going to do something.
		// Even if it's a repeated EOF message, it shouldn't be too big of a problem
		// to restart the cleaning cycle
		h.Mark = 0

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
				h.sendForward(h.broadcast(&NextStageMessage{
					Message:  msgFwd,
					Sequence: h.sequenceCounter,
					SentCallback: func() {
						// This guarantees that the only one that can trigger the EOF forward
						// sequence is the last one that has arrived, if there are others that are
						// repeated, they will not generate the sequence, as they will never update
						// and we will missing a token to start the sequence
						h.eofs.SaveState(eof.TokenName, msg.Message.IdemID())
						// Ack the EOF that generated the send forward to after everything was written
						msg.Delivery.Ack(false)
						h.signalFinish()
					},
				}, nil))
			}
		} else if updated && !finished {
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
	h.removeOnCleanup = true
}

func (h *HandlerRuntime) Finish() {
	// Ensure that the runtime has sent everything to the controller
	log.Debugf("Action: Received Finishing Signal to Finish %s-%s", h.ControllerName, h.JobId)
	<-h.finish
	log.Debugf("Action: Shutting Down %s-%s | Remove: %t", h.ControllerName, h.JobId, h.removeOnCleanup)
	h.handler.Shutdown(h.removeOnCleanup)
	log.Debugf("Action: Shutdown %s-%s", h.ControllerName, h.JobId)
}

func (h *HandlerRuntime) handleDataMessage(msg *messageFromQueue) {
	out, err := h.handler.Handle(msg.Message.Data(), msg.Message.IdemID())
	if err != nil {
		log.Errorf("Action: Handling Message %s - %s| Result: Error | Error: %s | Data: %s", h.ControllerName, h.JobId, err, msg.Message.Data())
		msg.Delivery.Nack(false, true)
	}
	if out != nil {
		h.sendForward(h.unicast(out, &msg.Delivery))
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
			m := h.unicast(r, nil)
			if m != nil {
				h.sendForward(m)
			}
		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			log.Errorf("Action: Next Stage Message %s - %s | Results: Error | Error: %s", h.ControllerName, h.JobId, err)
			return false
		}
	}
	return true
}

func (h *HandlerRuntime) unicast(m *NextStageMessage, d *amqp.Delivery) *messageToSend {
	h.sequenceCounter = m.Sequence
	if m.Message == nil {
		return nil
	}
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
