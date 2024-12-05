package business_test

import (
	"fmt"
	"middleware/common"
	"middleware/worker/business"
	"middleware/worker/schema"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/exp/rand"
)

func FatalOnError(err error, t *testing.T, message string) {
	if err != nil {
		t.Fatalf("%s: %s", message, err)
	}
}

func CreateRandomBatch(name string, n int, idx int) (*business.SortedBatch[*schema.NamedReviewCounter], error) {
	batch := business.NewSortedBatch[*schema.NamedReviewCounter](34*1024*1024, name)

	for i := 0; i < n; i++ {
		count := uint32(rand.Intn(1000))
		batch.Add(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("[%d] - Game N° %d - %d", idx, i, count),
			Count: count,
		})
	}

	return batch, batch.Save(func(nrc1, nrc2 *schema.NamedReviewCounter) bool {
		return nrc1.Count < nrc2.Count
	})
}

func CheckSortedFile(t *testing.T, file *common.TemporaryStorage) {
	file.Reset()

	scanner, err := file.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := schema.NamedReviewCounterDeserialize(d)
		return err
	})
	FatalOnError(err, t, "Cannot create scanner")

	lastReviewCount := -1

	for scanner.Scan() {
		line := scanner.Bytes()
		d := common.NewDeserializer(line)
		lineDes, err := schema.NamedReviewCounterDeserialize(&d)
		FatalOnError(err, t, fmt.Sprintf("Cannot deserialize review: %s", line))

		if lastReviewCount == -1 {
			lastReviewCount = int(lineDes.Count)
		} else if int(lineDes.Count) < lastReviewCount {
			t.Fatalf("Expected increasing order, got %d", lineDes.Count)
		}
	}

	t.Log("File sorted correctly")
}

func init() {
	os.RemoveAll(filepath.Join(".", "test_files"))
}
func TestQ5Insert(t *testing.T) {
	q5, err := business.NewQ5("test_files", "99", 1, 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	game := schema.NamedReviewCounter{
		Name:  "Game N° 1!",
		Count: 1,
	}

	q5.Insert(&game, &common.IdempotencyID{Origin: "A", Sequence: 1})

	nrcs, err := q5.Storage.ReadState(schema.NamedReviewCounterDeserialize)
	FatalOnError(err, t, "Cannot create scanner")

	for nrc := range nrcs {
		if nrc.Name != game.Name || nrc.Count != game.Count {
			t.Fatalf("Expected %v, got %v", game, nrc)
		} else {
			t.Log("Game inserted correctly")
		}
	}
}

func TestQ5CalculatePi(t *testing.T) {
	res := business.Q5CalculatePi(100, 90)

	if res != 90 {
		t.Fatalf("Expected 90, got %d", res)
	} else {
		t.Log("Pi calculated correctly")
	}
}

func TestQ5PartialSort(t *testing.T) {
	batch, err := CreateRandomBatch(filepath.Join(".", "test_files", "q5_partial_sort", "batch_100elems"), 100, 0)
	FatalOnError(err, t, "Cannot create random batch")

	tempFile, err := common.NewTemporaryStorage(batch.SaveTo)
	FatalOnError(err, t, "Cannot create temporary storage")

	CheckSortedFile(t, tempFile)
}

func TestQ5PushAtIdx1Row(t *testing.T) {
	h := business.NewHeap()

	batch, err := CreateRandomBatch(filepath.Join(".", "test_files", "q5_partial_sort", "batch_1000elems"), 1000, 0)
	FatalOnError(err, t, "Cannot create random batch")

	tempFile, err := common.NewTemporaryStorage(batch.SaveTo)
	FatalOnError(err, t, "Cannot create temporary storage")

	reader, err := tempFile.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := schema.NamedReviewCounterDeserialize(d)
		return err
	})
	FatalOnError(err, t, "Cannot create scanner")

	business.PushHeapAtIdx(h, reader, 0)

	d := common.NewDeserializer(reader.Bytes())
	firstRow, err := schema.NamedReviewCounterDeserialize(&d)
	FatalOnError(err, t, "Cannot deserialize review")

	t.Logf("First row: %v", firstRow)

	if h.Len() != 1 {
		t.Fatalf("Expected heap length 1, got %d", h.Len())
	} else {
		t.Log("Heap pushed correctly")
	}

	min := h.Pop().(business.ReviewWithSource)
	if min.Review.Count != firstRow.Count {
		t.Fatalf("Expected %d, got %d", firstRow.Count, min.Review.Count)
	} else {
		t.Log("Heap popped correctly")
	}
}

func TestQ5MergeSort(t *testing.T) {
	reviewsLen := 0

	tempSortedFiles := []*common.TemporaryStorage{}

	for i := 0; i < 10; i++ {
		batch, err := CreateRandomBatch(
			filepath.Join(".", "test_files", "q5_merge_sort", fmt.Sprintf("batch_%d", i)),
			1000,
			i)
		FatalOnError(err, t, "Cannot create temporary storage")
		reviewsLen += batch.Size

		tempFile, err := common.NewTemporaryStorage(batch.SaveTo)
		FatalOnError(err, t, "Cannot create temporary storage")

		tempSortedFiles = append(tempSortedFiles, tempFile)
	}

	sortedFile, err := common.NewTemporaryStorage(filepath.Join(".", "test_files", "q5_merge_sort", "result.sorted"))
	FatalOnError(err, t, "Cannot create temporary storage")

	reviewsLenResult, err := business.Q5MergeSort(sortedFile, tempSortedFiles)
	FatalOnError(err, t, "Cannot merge batches")

	if reviewsLen != reviewsLenResult {
		t.Fatalf("Expected %d reviews, got %d", reviewsLen, reviewsLenResult)
	} else {
		t.Log("Merge sort done completed correctly")
	}

	CheckSortedFile(t, sortedFile)
}

func TestQ5CalculateP90(t *testing.T) {
	q5, err := business.NewQ5("test_files", "99", 1, 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	for i := 100; i > 0; i-- {
		q5.Insert(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
		}, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(10000 - i + 1),
		})
	}

	idx, err := q5.Q5Quantile()
	FatalOnError(err, t, "Cannot calculate P90")

	if idx != 90 {
		t.Fatalf("Expected index 90, got %d", idx)
	} else {
		t.Log("P90 calculated correctly")
	}
}

func TestQ5CorrectResults(t *testing.T) {
	q5, err := business.NewQ5("test_files", "99", 1, 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	ga := 100
	fromExpected := business.Q5CalculatePi(ga, 90)
	for i := ga; i > 0; i-- {
		q5.Insert(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
		}, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(ga - i + 1),
		})
	}

	cr, ce := q5.NextStage()
	j := fromExpected + 1
	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break
			}
			if r.Message == nil {
				return
			}
			d := common.NewDeserializer(r.Message.Serialize())
			m, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			if m.Name != fmt.Sprintf("Game N° %d", j) {
				t.Fatalf("Game %s Reviews Expected: %d Got: %d", m.Name, j, m.Count)
			}
			j++

		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			t.Fatalf("There was an error while making the next stage %s", err)
		}
	}
}

func TestQ5CorrectResultsInterrupted(t *testing.T) {
	q5, err := business.NewQ5("test_files", "99", 1, 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	ga := 100
	fromExpected := business.Q5CalculatePi(ga, 90)
	for i := ga; i > 0; i-- {
		q5.Insert(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
		}, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(ga - i + 1),
		})
	}
	q5.Shutdown(false)
	q52, err := business.NewQ5("test_files", "99", 1, 90, 10)

	cr, ce := q52.NextStage()
	j := fromExpected + 1
	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break
			}
			if r.Message == nil {
				return
			}
			d := common.NewDeserializer(r.Message.Serialize())
			m, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			if m.Name != fmt.Sprintf("Game N° %d", j) {
				t.Fatalf("Game %s Reviews Expected: %d Got: %d", m.Name, j, m.Count)
			}
			j++

		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			t.Fatalf("There was an error while making the next stage %s", err)
		}
	}
}
