package common_test

import (
	"middleware/common"
	"os"
	"path/filepath"
	"testing"
)

type KeyedStateTest struct {
	key   string
	count uint32
}

func (s *KeyedStateTest) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteString(s.key).WriteUint32(s.count).ToBytes()
}

func KeyedStateTesDeserialize(d *common.Deserializer) (*KeyedStateTest, error) {
	app, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	c, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	return &KeyedStateTest{
		key:   app,
		count: c,
	}, nil
}

var mfhandler_test_files = filepath.Join(root_test_files, "handler_multiple_files")

func deleteMFHandlerTestFiles() {
	os.RemoveAll(mfhandler_test_files)
}

func init() {
	deleteMFHandlerTestFiles()
	os.Mkdir(root_test_files, 0755)
	os.Mkdir(mfhandler_test_files, 0755)
}

func prepareDir(dirname string) (*common.IdempotencyHandlerMultipleFiles[*StateTest], error) {
	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, dirname), 2)
	if err != nil {
		return nil, err
	}

	mfh.SaveState(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 1,
	}, &StateTest{
		count: 1,
	}, "1")

	mfh.SaveState(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 2,
	}, &StateTest{
		count: 1,
	}, "2")

	mfh.SaveState(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 3,
	}, &StateTest{
		count: 1,
	}, "1")

	mfh.SaveState(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 4,
	}, &StateTest{
		count: 1,
	}, "3")

	mfh.SaveState(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 5,
	}, &StateTest{
		count: 1,
	}, "2")

	return mfh, nil
}

func TestSaveMultiFile(t *testing.T) {
	mfh, err := prepareDir("save_test")
	if err != nil {
		t.Fatalf("Error while creating the handler %s", err)
	}

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 4,
	}) {
		t.Fatalf("The last IdemID for 'A' was not marked as the last processed")
	}

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 5,
	}) {
		t.Fatalf("The last IdemID for 'B' was not marked as the last processed")
	}

	if mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 6,
	}) {
		t.Fatalf("An Unknown ID was marked as already processed")
	}
}

func TestLoadMultiFile(t *testing.T) {
	_, err := prepareDir("load_test")
	if err != nil {
		t.Fatalf("Error while creating the handler %s", err)
	}

	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, "load_test"), 2)

	mfh.LoadState(StateTestDeserialize)

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 4,
	}) {
		t.Fatalf("The last IdemID for 'A' was not marked as the last processed")
	}

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 5,
	}) {
		t.Fatalf("The last IdemID for 'B' was not marked as the last processed")
	}

	if mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 6,
	}) {
		t.Fatalf("An Unknown ID was marked as already processed")
	}
}

func TestLoadMultiFileCorrupt(t *testing.T) {
	_, err := prepareDir("load_test_corrupt")
	if err != nil {
		t.Fatalf("Error while creating the handler %s", err)
	}

	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, "load_test_corrupt"), 2)

	corrupt_test_file(filepath.Join(mfhandler_test_files, "load_test_corrupt", mfh.GetFileName("3")), 2)

	mfh.LoadState(StateTestDeserialize)

	if mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 4,
	}) {
		t.Fatalf("The corrupted IdemID for 'A' was marked as the last processed")
	}

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "A",
		Sequence: 3,
	}) {
		t.Fatalf("The corrupted IdemID for 'A' was marked as the last processed")
	}

	if !mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 5,
	}) {
		t.Fatalf("The last IdemID for 'B' was not marked as the last processed")
	}

	if mfh.AlreadyProcessed(&common.IdempotencyID{
		Origin:   "B",
		Sequence: 6,
	}) {
		t.Fatalf("An Unknown ID was marked as already processed")
	}
}

func TestLoadState(t *testing.T) {
	mfh, _ := common.NewIdempotencyHandlerMultipleFiles[*KeyedStateTest](filepath.Join(mfhandler_test_files, "load_state"), 1)

	for i := 0; i < 10; i++ {
		mfh.SaveState(&common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(i),
		}, &KeyedStateTest{
			key:   "a",
			count: 1,
		}, "a")
	}

	for i := 0; i < 20; i++ {
		mfh.SaveState(&common.IdempotencyID{
			Origin:   "B",
			Sequence: uint32(i),
		}, &KeyedStateTest{
			key:   "b",
			count: 1,
		}, "b")
	}

	ca, _ := mfh.ReadSerialState("a", KeyedStateTesDeserialize, func(cs1, cs2 *KeyedStateTest) *KeyedStateTest {
		if cs2.key == "a" {
			cs1.count += cs2.count
		}
		return cs1
	}, &KeyedStateTest{key: "a", count: 0})

	cb, _ := mfh.ReadSerialState("b", KeyedStateTesDeserialize, func(cs1, cs2 *KeyedStateTest) *KeyedStateTest {
		if cs2.key == "b" {
			cs1.count += cs2.count
		}
		return cs1
	}, &KeyedStateTest{key: "b", count: 0})

	if ca.count != 10 {
		t.Fatalf("The count for the keyed state is not correct %d - %d", ca.count, 10)
	}

	if cb.count != 20 {
		t.Fatalf("The count for the keyed state is not correct %d - %d", cb.count, 20)
	}
}
