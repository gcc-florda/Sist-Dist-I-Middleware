package business

import "middleware/common"

type CountState struct {
	count uint32
}

func (s *CountState) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteUint32(s.count).ToBytes()
}

func CountStateDeserialize(d *common.Deserializer) (*CountState, error) {
	c, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	return &CountState{
		count: c,
	}, nil
}

func CountStateAggregate(old *CountState, new *CountState) *CountState {
	old.count += new.count
	return old
}

type NullState struct {
}

func (s *NullState) Serialize() []byte {
	return make([]byte, 0)
}

func NullStateDeserialize(d *common.Deserializer) (*NullState, error) {
	return &NullState{}, nil
}

func NullStateAggregate(old *NullState, new *NullState) *NullState {
	return old
}