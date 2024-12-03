package controller

import (
	"hash/fnv"
	"middleware/common"
	"middleware/worker/schema"
	"strconv"
)

const (
	ProtocolMessage_Data uint8 = iota
	ProtocolMessage_Control
)

type NodeProtocol struct {
	PartitionAmount uint
}

func (p *NodeProtocol) Unmarshal(rawData []byte) (DataMessage, error) {
	return common.MessageFromBytes(rawData)
}

func (p *NodeProtocol) Marshal(j common.JobID, idemId *common.IdempotencyID, d common.Serializable) (common.Serializable, error) {
	t := ProtocolMessage_Data
	if IsEOF(d) {
		t = ProtocolMessage_Control
		log.Debug("Escribo un EOF")
		return common.NewMessage(j, idemId, t, d.Serialize()), nil
	}

	data, err := schema.MarshalMessage(d)
	if err != nil {
		log.Fatalf("There was an error marshalling the message %s", err)
	}
	return common.NewMessage(j, idemId, t, data), nil
}

func (p *NodeProtocol) Route(partitionKey string) (routingKey string) {
	// Create a new FNV-1a hash
	h := fnv.New32a()
	h.Write([]byte(partitionKey))

	// Get the hash value as an unsigned integer
	hashValue := h.Sum32()

	// Map the hash value to a number between 1 and N
	// Add 1 to ensure it's in the range [1, N]
	return strconv.Itoa(int(hashValue%uint32(p.PartitionAmount)) + 1)
}

func (p *NodeProtocol) Broadcast() []string {
	numbers := make([]string, 0, p.PartitionAmount)

	for i := 1; i <= int(p.PartitionAmount); i++ {
		numbers = append(numbers, strconv.Itoa(i))
	}

	return numbers
}
