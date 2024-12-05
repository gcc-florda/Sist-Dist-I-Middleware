package business

import "middleware/common"

type sequence struct {
	nr uint32
}

func (s *sequence) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteUint32(s.nr).ToBytes()
}

func sequenceDeserialize(d *common.Deserializer) (*sequence, error) {
	seq, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	return &sequence{nr: seq}, nil
}

type FileSequence struct {
	lastSentSavedToDisk uint32
	storage             *common.TemporaryStorage
}

func readLastSequence(stg *common.TemporaryStorage) (*sequence, error) {
	sc, err := stg.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := sequenceDeserialize(d)
		return err
	})

	if err != nil {
		return nil, err
	}

	lastSequence := &sequence{}
	var okReadBytes uint32 = 0

	for sc.Scan() {
		b := sc.Bytes()
		d := common.NewDeserializer(b)
		seq, err := sequenceDeserialize(&d)
		if err != nil {
			break
		}

		lastSequence = seq
		okReadBytes += uint32(len(b))
	}
	common.CleanStorage(stg, okReadBytes)

	return lastSequence, nil
}

func NewFileSequence(path string) (*FileSequence, error) {
	stg, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	curr, err := readLastSequence(stg)
	if err != nil {
		return nil, err
	}
	return &FileSequence{
		lastSentSavedToDisk: curr.nr,
		storage:             stg,
	}, nil
}

func (s *FileSequence) LastConfirmedSent() uint32 {
	return s.lastSentSavedToDisk
}

func (s *FileSequence) Sent() {
	s.storage.Append((&sequence{nr: s.lastSentSavedToDisk}).Serialize())
}
