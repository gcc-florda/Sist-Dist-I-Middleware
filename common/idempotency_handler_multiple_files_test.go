package common_test

import (
	"middleware/common"
	"os"
	"path/filepath"
	"testing"
)

var mfhandler_test_files = filepath.Join(root_test_files, "handler_multiple_files")

func deleteMFHandlerTestFiles() {
	os.Remove(mfhandler_test_files)
}

func init() {
	deleteMFHandlerTestFiles()
	os.Mkdir(root_test_files, 0755)
	os.Mkdir(mfhandler_test_files, 0755)
}

func prepareDir(dirname string) (*common.IdempotencyHandlerMultipleFiles[*StateTest], error) {
	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, dirname))
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

	dir, err := os.Open(filepath.Join(mfhandler_test_files, "save_test"))
	if err != nil {
		t.Fatalf("failed to open directory: %s", err)
	}
	defer dir.Close()

	files, err := dir.Readdir(-1) // Read all entries
	if err != nil {
		t.Fatalf("failed to read directory: %s", err)
	}

	count := 0
	for _, file := range files {
		if file.Mode().IsRegular() {
			count++
		}
	}

	if count != 3 {
		t.Fatalf("The amount of files created is different than expected %d - %d", count, 3)
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

	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, "load_test"))

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

	corrupt_test_file(filepath.Join(mfhandler_test_files, "load_test_corrupt", "3"), 2)

	mfh, err := common.NewIdempotencyHandlerMultipleFiles[*StateTest](filepath.Join(mfhandler_test_files, "load_test_corrupt"))

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
