package common

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type TemporaryStorage struct {
	filepath string
	file     *os.File
	term     chan os.Signal
}

type ClosedFileError struct {
	filepath string
}

func (m *ClosedFileError) Error() string {
	return fmt.Sprintf("The file %s was already closed. This can be due to manual Close() call or due to a SYSCALL received", m.filepath)
}

func NewTemporaryStorage(path string) (*TemporaryStorage, error) {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	t := &TemporaryStorage{
		filepath: path,
		file:     f,
		term:     make(chan os.Signal, 1),
	}

	signal.Notify(t.term, syscall.SIGTERM)
	go t.handleShutdown()
	return t, nil
}

func (t *TemporaryStorage) Close() {
	if t.file == nil {
		return
	}

	err := t.file.Close()
	if err != nil {
		log.Errorf("Failed to close correctly the file %s: %s", t.filepath, err)
	}
	t.file = nil
}

func (t *TemporaryStorage) handleShutdown() {
	<-t.term
	t.Close()
}

func (t *TemporaryStorage) Overwrite(data []byte) (int, error) {
	if t.file == nil {
		return -1, &ClosedFileError{
			filepath: t.filepath,
		}
	}
	t.file.Truncate(0)
	t.Reset()
	return t.file.Write(data)
}

func (t *TemporaryStorage) SaveState(state Serializable) (int, error) {
	return t.Overwrite(state.Serialize())
}

func (t *TemporaryStorage) Append(data []byte) (int, error) {
	if t.file == nil {
		return -1, &ClosedFileError{
			filepath: t.filepath,
		}
	}
	return t.file.Write(data)
}

func (t *TemporaryStorage) AppendLine(data []byte) (int, error) {
	return t.Append(append(append(data, '@'), '*'))
}

func (t *TemporaryStorage) Reset() {
	if t.file == nil {
		return
	}
	t.file.Seek(0, io.SeekStart)
}

func (t *TemporaryStorage) ReadAll() ([]byte, error) {
	t.Reset()
	if t.file == nil {
		return nil, &ClosedFileError{
			filepath: t.filepath,
		}
	}

	b, err := io.ReadAll(t.file)
	return b, err
}

func LoadState[T any](t *TemporaryStorage, d Deserialize[T]) (T, error) {
	ds, err := t.ReadAll()
	if err != nil {
		var zeroValue T
		return zeroValue, err
	}
	return d(ds)
}

func (t *TemporaryStorage) File() (*os.File, error) {
	if t.file == nil {
		return nil, &ClosedFileError{
			filepath: t.filepath,
		}
	}

	return t.file, nil
}

func (t *TemporaryStorage) Scanner() (*bufio.Scanner, error) {
	if t.file == nil {
		return nil, &ClosedFileError{
			filepath: t.filepath,
		}
	}

	s := bufio.NewScanner(t.file)
	s.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		// Look for the first occurrence of '@*'
		if i := bytes.Index(data, []byte("@*")); i >= 0 {
			// Return the data up to the delimiter '@*'
			return i + 2, data[:i], nil
		}
		// If we're at EOF and there's remaining data, return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	})

	return s, nil
}
