package common

type IdempotencyID uint32

func SaveState(caused_by IdempotencyID, state Serializable, storage *TemporaryStorage) error {
	s := NewSerializer()
	b := s.WriteUint32(uint32(caused_by)).WriteBytes(state.Serialize()).ToBytes()
	_, err := storage.Append(b)
	if err != nil {
		return err
	}
	return nil
}

func LoadSavedState[T any](stg *TemporaryStorage, des func(*Deserializer) (T, error), agg func(T, T) T, initial T) (IdempotencyID, T, error) {
	var lastId IdempotencyID
	var lastState T = initial
	scanner, err := stg.ScannerDeserialize(scannerFunc(des))
	if err != nil {
		return lastId, lastState, err
	}

	var okReadBytes uint32 = 0
	for scanner.Scan() {
		b := scanner.Bytes()
		d := NewDeserializer(b)
		id, err := d.ReadUint32()
		if err != nil {
			// We can't read the line, it means that the line is corrupted
			// Clean it up, return the lastId and the lastState succesfully read
			_ = cleanStorage(stg, okReadBytes)
			return lastId, lastState, nil
		}
		s, err := des(&d)
		if err != nil {
			// We can't read the line, it means that the line is corrupted
			// Clean it up, return the lastId and the lastState succesfully read
			_ = cleanStorage(stg, okReadBytes)
			return lastId, lastState, nil
		}

		// We can read the state correctly, along with the IdempotencyID that produced it
		// This is the last one that we have to save, and because is overwritten state, just sum it up
		// We have read this line completely, so
		lastId = IdempotencyID(id)
		if agg != nil {
			lastState = agg(lastState, s)
		} else {
			lastState = s
		}
		okReadBytes += uint32(len(b))
	}
	cleanStorage(stg, okReadBytes)
	return lastId, lastState, nil
}

func scannerFunc[T any](f func(*Deserializer) (T, error)) func(d *Deserializer) error {
	return func(d *Deserializer) error {
		_, err := d.ReadInt32() //Read IdempotencyID
		if err != nil {
			return err
		}
		_, err = f(d) //Read State
		if err != nil {
			return err
		}
		return nil
	}
}

func cleanStorage(storage *TemporaryStorage, clean_until uint32) error {
	storage.Reset()
	f, err := storage.File()
	if err != nil {
		return err
	}
	f.Truncate(int64(clean_until))
	return nil
}
