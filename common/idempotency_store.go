package common

import (
	"errors"
)

type IdempotencyStore struct {
	last_id_per_og map[string]*IdempotencyID
}

func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		last_id_per_og: make(map[string]*IdempotencyID),
	}
}

func (s *IdempotencyStore) Save(id *IdempotencyID) {
	og := id.origin
	s.last_id_per_og[og] = id
}

func (s *IdempotencyStore) AlreadyProcessed(id *IdempotencyID) bool {
	og := id.origin
	last, ok := s.last_id_per_og[og]
	if !ok {
		return false
	}
	return last.identifier == id.identifier
}

func (s *IdempotencyStore) LastForOrigin(og string) (*IdempotencyID, error) {
	last, ok := s.last_id_per_og[og]
	if !ok {
		return nil, errors.New("the given origin doesn't have a IdempotencyID")
	}
	return last, nil
}
