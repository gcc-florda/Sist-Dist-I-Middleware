package common_test

import (
	"log"
	"middleware/common"
	"os"
	"path/filepath"
	"testing"
)

type StateTest struct {
	count uint32
}

func (s *StateTest) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteUint32(s.count).ToBytes()
}

func StateTestDeserialize(d *common.Deserializer) (*StateTest, error) {
	c, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	return &StateTest{
		count: c,
	}, nil
}

var test_files = filepath.Join(".", "test_files")
var test_files_save = filepath.Join(test_files, "save_state")

func deleteFiles() {
	os.Remove(test_files)
}

func write_to_test_file(name string, data []byte) {
	file, err := os.OpenFile(filepath.Join(test_files, name), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Failed to open or create file: %v", err)
	}
	defer file.Close()

	file.Write(data)
}

func corrupt_test_file(name string, how_much uint32) {
	fp := filepath.Join(test_files, name)
	fi, err := os.Stat(fp)
	if err != nil {
		log.Fatalf("Failed to open or create file: %v", err)
	}
	file, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Failed to open or create file: %v", err)
	}
	defer file.Close()

	file.Truncate(fi.Size() - int64(how_much))
}

func init() {
	deleteFiles()
	os.Mkdir(test_files, 0755)
	os.Mkdir(test_files_save, 0755)
	s := common.NewSerializer()

	b := s.
		WriteUint32(1).WriteUint32(1).
		WriteUint32(2).WriteUint32(2).
		WriteUint32(3).WriteUint32(3).
		WriteUint32(4).WriteUint32(4).
		WriteUint32(5).WriteUint32(5).
		ToBytes()

	write_to_test_file("sequential_state_ok", b)

	write_to_test_file("overwrite_state_ok", b)

	write_to_test_file("sequential_state_corrupt", b)
	corrupt_test_file("sequential_state_corrupt", 2)

	write_to_test_file("overwrite_state_corrupt", b)
	corrupt_test_file("overwrite_state_corrupt", 2)
}

func TestLoadSavedStateSequentialOk(t *testing.T) {
	stg, err := common.NewTemporaryStorage(filepath.Join(test_files, "sequential_state_ok"))
	if err != nil {
		t.Fatalf("Can't create temporary storage")
	}
	lastId, state, err := common.LoadSavedState(
		stg,
		StateTestDeserialize,
		func(old *StateTest, new *StateTest) *StateTest {
			old.count += new.count
			return old
		},
		&StateTest{
			count: 0,
		},
	)

	if lastId != 5 {
		t.Fatalf("The last read IdempotencyID is not the expected %d - %d", lastId, 5)
	}

	if state.count != 15 {
		t.Fatalf("The state read is not the expected %d - %d", state.count, 15)
	}
}

func TestLoadSavedStateOverwriteOk(t *testing.T) {
	stg, err := common.NewTemporaryStorage(filepath.Join(test_files, "overwrite_state_ok"))
	if err != nil {
		t.Fatalf("Can't create temporary storage")
	}
	lastId, state, err := common.LoadSavedState(
		stg,
		StateTestDeserialize,
		nil,
		&StateTest{
			count: 0,
		},
	)

	if lastId != 5 {
		t.Fatalf("The last read IdempotencyID is not the expected %d - %d", lastId, 5)
	}

	if state.count != 5 {
		t.Fatalf("The state read is not the expected %d - %d", state.count, 5)
	}
}

func TestLoadSavedStateSequentialCorrupt(t *testing.T) {
	stg, err := common.NewTemporaryStorage(filepath.Join(test_files, "sequential_state_corrupt"))
	if err != nil {
		t.Fatalf("Can't create temporary storage")
	}
	lastId, state, err := common.LoadSavedState(
		stg,
		StateTestDeserialize,
		nil,
		&StateTest{
			count: 0,
		},
	)

	if lastId != 4 {
		t.Fatalf("The last read IdempotencyID is not the expected %d - %d", lastId, 4)
	}

	if state.count != 4 {
		t.Fatalf("The state read is not the expected %d - %d", state.count, 10)
	}

	fi, err := os.Stat(filepath.Join(test_files, "sequential_state_corrupt"))
	if err != nil {
		log.Fatalf("Failed to open or create file: %v", err)
	}
	// 4 bytes per uint32, 2 uint32 per line, 4 lines non corrupt
	if fi.Size() != (4 * 2 * 4) {
		log.Fatalf("The corrupt state was not truncated correctly %d - %d", fi.Size(), 4*2*4)
	}

}

func TestLoadSavedStateOverwriteCorrupt(t *testing.T) {
	stg, err := common.NewTemporaryStorage(filepath.Join(test_files, "overwrite_state_corrupt"))
	if err != nil {
		t.Fatalf("Can't create temporary storage")
	}
	lastId, state, err := common.LoadSavedState(
		stg,
		StateTestDeserialize,
		nil,
		&StateTest{
			count: 0,
		},
	)

	if lastId != 4 {
		t.Fatalf("The last read IdempotencyID is not the expected %d - %d", lastId, 4)
	}

	if state.count != 4 {
		t.Fatalf("The state read is not the expected %d - %d", state.count, 4)
	}

	fi, err := os.Stat(filepath.Join(test_files, "overwrite_state_corrupt"))
	if err != nil {
		log.Fatalf("Failed to open or create file: %v", err)
	}
	// 4 bytes per uint32, 2 uint32 per line, 4 lines non corrupt
	if fi.Size() != (4 * 2 * 4) {
		log.Fatalf("The corrupt state was not truncated correctly %d - %d", fi.Size(), 4*2*4)
	}

}
