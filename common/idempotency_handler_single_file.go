package common

type IdempotencyHandlerSingleFile[T Serializable] struct {
	filename  string
	idemStore *IdempotencyStore
	storage   *TemporaryStorage
}

func NewIdempotencyHandlerSingleFile[T Serializable](
	filename string,
) (*IdempotencyHandlerSingleFile[T], error) {
	s, err := NewTemporaryStorage(filename)
	if err != nil {
		return nil, err
	}

	return &IdempotencyHandlerSingleFile[T]{
		filename:  filename,
		idemStore: nil,
		storage:   s,
	}, nil
}

func (h *IdempotencyHandlerSingleFile[T]) LoadSequentialState(
	des func(*Deserializer) (T, error),
	agg func(T, T) T,
	initial T,
) (T, error) {
	var zT T
	store, state, err := LoadSavedState(h.storage, des, agg, initial)
	if err != nil {
		return zT, err
	}
	h.idemStore = store
	return state, nil
}

func (h *IdempotencyHandlerSingleFile[T]) LoadOverwriteState(
	des func(*Deserializer) (T, error),
) (T, error) {
	var zT T
	store, state, err := LoadSavedState(h.storage, des, nil, zT)
	if err != nil {
		return zT, err
	}
	h.idemStore = store
	return state, nil
}

func (h *IdempotencyHandlerSingleFile[T]) SaveState(caused_by *IdempotencyID, state T) error {
	err := SaveState(caused_by, state, h.storage)
	if err != nil {
		return err
	}
	h.idemStore.Save(caused_by)
	return nil
}

func (h *IdempotencyHandlerSingleFile[T]) ReadState(des func(*Deserializer) (T, error)) (<-chan T, error) {
	h.storage.Reset()
	rs, err := ReadState(h.storage, des)
	if rs != nil {
		return nil, err
	}
	ch := make(chan T)
	go func() {
		for line := range rs {
			ch <- (line.data)
		}
	}()

	return ch, nil
}

func (h *IdempotencyHandlerSingleFile[T]) AlreadyProcessed(idemId *IdempotencyID) bool {
	return h.idemStore.AlreadyProcessed(idemId)
}

func (h *IdempotencyHandlerSingleFile[T]) Close() {
	h.storage.Close()
}

func (h *IdempotencyHandlerSingleFile[T]) Delete() error {
	return h.storage.Delete()
}
