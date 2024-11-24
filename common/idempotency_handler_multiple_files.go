package common

import (
	"fmt"
	"os"
	"path/filepath"
)

type fileManager struct {
	dirname string
}

func ensureDir(dirname string) error {
	// Check if the directory exists
	info, err := os.Stat(dirname)
	if err != nil {
		// If the directory doesn't exist, try to create it
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755) // 0755 permissions (rwxr-xr-x)
			if err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		} else {
			return fmt.Errorf("error checking directory: %w", err)
		}
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dirname)
	}

	// Directory exists and is valid, or was created successfully
	return nil
}
func newFileManager(dirname string) (*fileManager, error) {
	err := ensureDir(dirname)
	if err != nil {
		return nil, err
	}
	return &fileManager{dirname: dirname}, nil
}

func (fm *fileManager) Files() (<-chan *TemporaryStorage, error) {
	dir, err := os.Open(fm.dirname)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	filesChan := make(chan *TemporaryStorage)

	go func() {
		defer close(filesChan)

		files, err := dir.Readdirnames(0)
		if err != nil {
			return
		}

		for _, file := range files {
			fullPath := filepath.Join(fm.dirname, file)
			storage, err := NewTemporaryStorage(fullPath)
			if err == nil {
				filesChan <- storage
			}
		}
	}()

	return filesChan, nil
}

func (fm *fileManager) Open(filename string) (*TemporaryStorage, error) {
	return NewTemporaryStorage(filepath.Join(fm.dirname, filename))
}

type IdempotencyHandlerMultipleFiles[T Serializable] struct {
	idemStore   *IdempotencyStore
	filemanager *fileManager
}

func NewIdempotencyHandlerMultipleFiles[T Serializable](dirname string) (*IdempotencyHandlerMultipleFiles[T], error) {
	fm, err := newFileManager(dirname)
	if err != nil {
		return nil, err
	}
	return &IdempotencyHandlerMultipleFiles[T]{
		idemStore:   NewIdempotencyStore(),
		filemanager: fm,
	}, nil
}

func (h *IdempotencyHandlerMultipleFiles[T]) LoadState(
	des func(*Deserializer) (T, error),
) error {
	var zT T

	files, err := h.filemanager.Files()
	if err != nil {
		return err
	}

	for file := range files {
		// For now, we only care about the IdempotencyIDs
		// Just try to keep the memory footprint of the state to a minimum
		store, _, err := LoadSavedState(file, des, nil, zT)
		if err != nil {
			return err
		}
		h.idemStore.Merge(store)
	}

	return nil
}

func (h *IdempotencyHandlerMultipleFiles[T]) SaveState(caused_by *IdempotencyID, state T, where string) error {
	storage, err := h.filemanager.Open(where)
	if err != nil {
		return err
	}
	err = SaveState(caused_by, state, storage)
	if err != nil {
		return err
	}
	h.idemStore.Save(caused_by)
	return nil
}

func (h *IdempotencyHandlerMultipleFiles[T]) AlreadyProcessed(idemId *IdempotencyID) bool {
	return h.idemStore.AlreadyProcessed(idemId)
}
