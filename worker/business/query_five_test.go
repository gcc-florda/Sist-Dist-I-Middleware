package business_test

import (
	"fmt"
	"middleware/common"
	"middleware/worker/business"
	"middleware/worker/schema"
	"path/filepath"
	"testing"

	"golang.org/x/exp/rand"
)

func FatalOnError(err error, t *testing.T, message string) {
	if err != nil {
		t.Fatalf("%s: %s", message, err)
	}
}

func CreateRandomBatch(n int, idx int) *schema.NamedReviewBatch {
	batch := schema.NewNamedReviewBatch("temp", "99", idx)

	for i := 0; i < n; i++ {
		count := uint32(rand.Intn(1000))
		batch.Add(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("[%d] - Game N° %d - %d", idx, i, count),
			Count: count,
		})
	}

	return batch
}

func CreateRandomBatchSorted(n int, idx int) (*schema.NamedReviewBatch, *common.TemporaryStorage, error) {
	batch := schema.NewNamedReviewBatch("temp", "99", idx)

	for i := 0; i < n; i++ {
		count := uint32(rand.Intn(1000))
		batch.Add(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("[%d] - Game N° %d - %d", idx, i, count),
			Count: count,
		})
	}

	file, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", "temp", fmt.Sprintf("batch_%d", batch.Index)))
	if err != nil {
		return nil, nil, err
	}
	file.Overwrite([]byte{})

	err = business.Q5PartialSort(batch, file)
	if err != nil {
		return nil, nil, err
	}

	file.Reset()

	return batch, file, nil
}

func CheckSortedFile(t *testing.T, file *common.TemporaryStorage) {
	file.Reset()

	scanner, err := file.Scanner()
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
func TestQ5Insert(t *testing.T) {
	q5, err := business.NewQ5("temp", "99", 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	q5.Storage.Overwrite([]byte{})

	game := schema.NamedReviewCounter{
		Name:  "Game N° 1!",
		Count: 1,
	}

	q5.Insert(&game)

	scanner, err := q5.Storage.Scanner()
	FatalOnError(err, t, "Cannot create scanner")

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		lineDes, err := schema.NamedReviewCounterDeserialize(&d)
		FatalOnError(err, t, "Cannot deserialize review")

		if lineDes.Name != game.Name || lineDes.Count != game.Count {
			t.Fatalf("Expected %v, got %v", game, lineDes)
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
	batch := CreateRandomBatch(100, 0)

	tempFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", fmt.Sprintf("batch_%d", batch.Index)))
	FatalOnError(err, t, "Cannot create temporary storage")
	tempFile.Overwrite([]byte{})

	err = business.Q5PartialSort(batch, tempFile)
	FatalOnError(err, t, "Cannot sort batch")

	CheckSortedFile(t, tempFile)
}

func TestQ5PushAtIdx1Row(t *testing.T) {
	h := business.NewHeap()

	batch := CreateRandomBatch(100, 0)

	tempFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", fmt.Sprintf("batch_%d", batch.Index)))
	FatalOnError(err, t, "Cannot create temporary storage")
	tempFile.Overwrite([]byte{})

	err = business.Q5PartialSort(batch, tempFile)
	FatalOnError(err, t, "Cannot sort batch")

	tempFile.Reset()

	reader, err := tempFile.Scanner()
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
		batch := CreateRandomBatch(100000, i)
		reviewsLen += batch.Size

		tempFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", fmt.Sprintf("batch_%d", batch.Index)))
		FatalOnError(err, t, "Cannot create temporary storage")
		tempFile.Overwrite([]byte{})

		err = business.Q5PartialSort(batch, tempFile)
		FatalOnError(err, t, "Cannot sort batch")

		tempSortedFiles = append(tempSortedFiles, tempFile)
	}

	sortedFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", "merge_sort"))
	FatalOnError(err, t, "Cannot create temporary storage")
	sortedFile.Overwrite([]byte{})

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
	q5, err := business.NewQ5("temp", "99", 90, 10)
	FatalOnError(err, t, "Cannot create Q5")

	q5.Storage.Overwrite([]byte{})

	for i := 100; i > 0; i-- {
		q5.Insert(&schema.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
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
