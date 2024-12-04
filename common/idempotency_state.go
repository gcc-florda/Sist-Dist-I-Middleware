package common

func SaveState(caused_by *IdempotencyID, state Serializable, storage *TemporaryStorage) error {
	s := NewSerializer()
	b := s.WriteBytes(caused_by.Serialize()).WriteBytes(state.Serialize()).ToBytes()
	_, err := storage.Append(b)
	if err != nil {
		return err
	}
	return nil
}

func LoadSavedState[T any](stg *TemporaryStorage, des func(*Deserializer) (T, error), agg func(T, T) T, initial T) (*IdempotencyStore, T, error) {
	lastIds := NewIdempotencyStore()
	var lastState T = initial
	rs, err := ReadState(stg, des)
	if err != nil {
		return lastIds, lastState, err
	}
	for line := range rs {
		lastIds.Save(line.id)
		if agg != nil {
			lastState = agg(lastState, line.data)
		} else {
			lastState = line.data
		}
	}
	return lastIds, lastState, nil
}

func ReadState[T any](stg *TemporaryStorage, des func(*Deserializer) (T, error)) (<-chan *struct {
	id   *IdempotencyID
	data T
}, error) {
	stg.Reset()
	s := make(chan *struct {
		id   *IdempotencyID
		data T
	}, 10)
	scanner, err := stg.ScannerDeserialize(scannerFunc(des))
	if err != nil {
		return nil, err
	}

	go func() {
		var okReadBytes uint32 = 0
		defer close(s)

		for scanner.Scan() {
			b := scanner.Bytes()
			d := NewDeserializer(b)
			id, err := IdempotencyIDDeserialize(&d)
			if err != nil {
				// We can't read the line, it means that the line is corrupted
				return
			}
			data, err := des(&d)
			if err != nil {
				// We can't read the line, it means that the line is corrupted
				return
			}

			s <- &struct {
				id   *IdempotencyID
				data T
			}{id: id, data: data}

			okReadBytes += uint32(len(b))
		}

		CleanStorage(stg, okReadBytes)
	}()
	return s, nil
}

func scannerFunc[T any](f func(*Deserializer) (T, error)) func(d *Deserializer) error {
	return func(d *Deserializer) error {
		_, err := IdempotencyIDDeserialize(d) //Read IdempotencyID
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

func CleanStorage(storage *TemporaryStorage, clean_until uint32) error {
	storage.Reset()
	f, err := storage.File()
	if err != nil {
		return err
	}
	f.Truncate(int64(clean_until))
	return nil
}
