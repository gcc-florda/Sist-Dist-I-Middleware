package business_test

import (
	"middleware/worker/business"
	"os"
	"path/filepath"
	"testing"
)

func init() {
	os.RemoveAll(filepath.Join(".", "test_files"))
}

func TestInterruptedCount(t *testing.T) {
	fSeq, _ := business.NewFileSequence(filepath.Join(".", "test_files", "file_sequence", "interrupt"))

	result := make([]int, 0, 100)
	for i := 1; i <= 50; i++ {
		if i <= int(fSeq.LastConfirmedSent()) {
			continue
		}
		result = append(result, i)
		fSeq.Sent()
	}

	fSeq.Shutdown(false)

	fInt, _ := business.NewFileSequence(filepath.Join(".", "test_files", "file_sequence", "interrupt"))
	if int(fInt.LastConfirmedSent()) != len(result) {
		t.Fatalf("Partial count is not as expected: %d - %d", int(fInt.LastConfirmedSent()), len(result))
	}
	for i := 1; i <= 100; i++ {
		if i <= int(fInt.LastConfirmedSent()) {
			continue
		}
		result = append(result, i)
		fInt.Sent()
	}

	if len(result) != 100 {
		t.Fatalf("Not the expected amount sent: %d - %d", len(result), 100)
	}
}

func TestInterruptedAtEndCount(t *testing.T) {
	fSeq, _ := business.NewFileSequence(filepath.Join(".", "test_files", "file_sequence", "interrupt_end"))

	result := make([]int, 0, 100)
	for i := 1; i <= 100; i++ {
		if i <= int(fSeq.LastConfirmedSent()) {
			continue
		}
		result = append(result, i)
		fSeq.Sent()
	}
	fSeq.Shutdown(false)

	fInt, _ := business.NewFileSequence(filepath.Join(".", "test_files", "file_sequence", "interrupt_end"))
	if int(fInt.LastConfirmedSent()) != len(result) {
		t.Fatalf("Partial count is not as expected: %d - %d", int(fInt.LastConfirmedSent()), len(result))
	}
	for i := 1; i <= 100; i++ {
		if i <= int(fInt.LastConfirmedSent()) {
			continue
		}
		result = append(result, i)
		fInt.Sent()
	}

	if len(result) != 100 {
		t.Fatalf("Not the expected amount sent: %d - %d", len(result), 100)
	}
}
