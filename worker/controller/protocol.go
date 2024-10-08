package controller

import (
	"errors"
	"hash/fnv"
	"middleware/common"
	"strconv"
)

const (
	ProtocolMessage_Data uint8 = iota
	ProtocolMessage_Control
)

type ProtocolMessage struct {
	jobID JobID
	t     uint8
	data  []byte
}

func (pm *ProtocolMessage) Serialize() []byte {
	s := common.NewSerializer()
	return s.WriteString(pm.jobID).WriteBytes(pm.data).ToBytes()
}

func (pm *ProtocolMessage) JobID() JobID {
	return pm.jobID
}

func (pm *ProtocolMessage) IsEOF() bool {
	return pm.t == ProtocolMessage_Control
}

func (pm *ProtocolMessage) Data() []byte {
	return pm.data
}

type NodeProtocol struct {
	PartitionAmount uint
}

func (p *NodeProtocol) Unmarshal(rawData []byte) (DataMessage, error) {
	d := common.NewDeserializer(rawData)
	jobId, err := d.ReadString()
	if err != nil {
		return nil, err
	}
	t, err := d.ReadUint8()
	if err != nil {
		return nil, err
	}
	if t == ProtocolMessage_Data || t == ProtocolMessage_Control {
		return &ProtocolMessage{
			jobID: jobId,
			t:     ProtocolMessage_Data,
			data:  rawData[len(rawData)-d.Buf.Len():],
		}, nil
	}
	return nil, errors.New("the read message from the protocol is not of a known type")
}

func (p *NodeProtocol) Marshal(j JobID, d common.Serializable) (common.Serializable, error) {
	t := ProtocolMessage_Data
	if IsEOF(d) {
		t = ProtocolMessage_Control
	}
	data := d.Serialize()
	return &ProtocolMessage{
		jobID: j,
		t:     t,
		data:  data,
	}, nil
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
	numbers := make([]string, p.PartitionAmount)

	for i := 1; i <= int(p.PartitionAmount); i++ {
		numbers = append(numbers, strconv.Itoa(i))
	}

	return numbers
}
