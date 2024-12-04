package common

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
)

type cacheNode struct {
	data   *TemporaryStorage
	keyPtr *list.Element
}

type stgCache struct {
	queue     *list.List
	cachesize int
	items     map[string]*cacheNode
}

func (c *stgCache) put(key string, value *TemporaryStorage) {
	if item, ok := c.items[key]; !ok {
		if c.cachesize == len(c.items) {
			back := c.queue.Back()
			c.queue.Remove(back)
			stg := c.items[back.Value.(string)]
			stg.data.Close()
			delete(c.items, back.Value.(string))
		}
		c.items[key] = &cacheNode{data: value, keyPtr: c.queue.PushFront(key)}
	} else {
		item.data = value
		c.items[key] = item
		c.queue.MoveToFront(item.keyPtr)
	}
}

func (l *stgCache) get(key string) *TemporaryStorage {
	if item, ok := l.items[key]; ok {
		l.queue.MoveToFront(item.keyPtr)
		return item.data
	}
	return nil
}

func (l *stgCache) clear() {
	for k := range l.items {
		l.items[k].data.Close()
		l.queue.Remove(l.items[k].keyPtr)
		delete(l.items, k)
	}

}

type fileManager struct {
	dirname string
	cache   *stgCache
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
func newFileManager(dirname string, cacheSize uint32) (*fileManager, error) {
	err := ensureDir(dirname)
	if err != nil {
		return nil, err
	}
	return &fileManager{dirname: dirname, cache: &stgCache{
		queue:     list.New(),
		cachesize: int(cacheSize),
		items:     make(map[string]*cacheNode),
	}}, nil
}

func (fm *fileManager) Files() (<-chan *TemporaryStorage, error) {
	dir, err := os.Open(fm.dirname)
	if err != nil {
		return nil, err
	}

	filesChan := make(chan *TemporaryStorage)

	go func() {
		defer close(filesChan)
		defer dir.Close()

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
	if v := fm.cache.get(filename); v != nil {
		return v, nil
	}
	n, err := NewTemporaryStorage(filepath.Join(fm.dirname, filename))
	if err != nil {
		return nil, err
	}
	fm.cache.put(filename, n)
	return n, nil

}

func (fm *fileManager) Close() {
	fm.cache.clear()
}

func (fm *fileManager) Delete() error {
	return os.Remove(fm.dirname)
}

type IdempotencyHandlerMultipleFiles[T Serializable] struct {
	idemStore   *IdempotencyStore
	filemanager *fileManager
	nrFiles     uint32
}

func NewIdempotencyHandlerMultipleFiles[T Serializable](dirname string, N uint32) (*IdempotencyHandlerMultipleFiles[T], error) {
	fm, err := newFileManager(dirname, N/2)
	if err != nil {
		return nil, err
	}
	return &IdempotencyHandlerMultipleFiles[T]{
		idemStore:   NewIdempotencyStore(),
		filemanager: fm,
		nrFiles:     N,
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
		file.Close()
	}

	return nil
}

func (h *IdempotencyHandlerMultipleFiles[T]) SaveState(caused_by *IdempotencyID, state T, key string) error {
	storage, err := h.filemanager.Open(h.GetFileName(key))
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

func (h *IdempotencyHandlerMultipleFiles[T]) ReadSerialState(
	key string,
	des func(*Deserializer) (T, error),
	agg func(T, T) T,
	initial T,
) (T, error) {
	f, err := h.filemanager.Open(h.GetFileName(key))
	if err != nil {
		return initial, err
	}

	_, state, err := LoadSavedState(f, des, agg, initial)
	if err != nil {
		return initial, err
	}
	return state, nil
}

func (h *IdempotencyHandlerMultipleFiles[T]) AlreadyProcessed(idemId *IdempotencyID) bool {
	return h.idemStore.AlreadyProcessed(idemId)
}

func (h *IdempotencyHandlerMultipleFiles[T]) Close() {
	h.filemanager.Close()
}

func (h *IdempotencyHandlerMultipleFiles[T]) Delete() error {
	return h.filemanager.Delete()
}

func (h *IdempotencyHandlerMultipleFiles[T]) GetFileName(key string) string {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return fmt.Sprint((hash % h.nrFiles) + 1)
}
