package common

import (
	"bufio"
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

	t.file.Close()
	signal.Stop(t.term)
	t.term <- syscall.SIGTERM
	t.file = nil
}

func (t *TemporaryStorage) Delete() error {
	t.Close()
	err := os.Remove(t.filepath)
	if err != nil {
		return err
	}
	return nil
}

func (t *TemporaryStorage) handleShutdown() {
	<-t.term
	t.Close()
	close(t.term)
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
	t.file.Seek(0, io.SeekEnd)
	w, err := t.file.Write(data)
	if err != nil {
		return w, err
	}
	// err = t.file.Sync()
	// if err != nil {
	// 	return w, err
	// }
	return w, nil
}

func (t *TemporaryStorage) AppendLine(data []byte) (int, error) {
	return t.Append(append(data, '\n'))
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

	return s, nil
}

func (t *TemporaryStorage) ScannerDeserialize(f func(*Deserializer) error) (*bufio.Scanner, error) {
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

		d := NewDeserializer(data)
		a := f(&d)
		if a != nil {
			return 0, nil, nil
		}
		l := len(data) - d.Buf.Len()

		if l <= 0 {
			return len(data), data, nil
		}

		return l, data[:l], nil
	})

	return s, nil
}
