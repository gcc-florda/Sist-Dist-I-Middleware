package business_test

import (
	"fmt"
	"middleware/common"
	"middleware/worker/business"
	"path/filepath"
	"testing"

	"golang.org/x/exp/rand"
)

func FatalOnError(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func CreateRandomBatch(n int, idx int) *business.NamedReviewBatch {
	batch := business.NewNamedReviewBatch("temp", "99", idx)

	for i := n; i > 0; i-- {
		batch.Add(&business.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(rand.Intn(1000)),
		})
	}

	return batch
}

func TestQ5Insert(t *testing.T) {
	q5, err := business.NewQ5("temp", "99", 90, 10)
	FatalOnError(err, t)

	q5.Storage.Overwrite([]byte{})

	game := business.NamedReviewCounter{
		Name:  "Game N° 1!",
		Count: 1,
	}

	q5.Insert(&game)

	scanner, err := q5.Storage.Scanner()
	FatalOnError(err, t)

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		lineDes, err := business.NamedReviewCounterDeserialize(&d)
		FatalOnError(err, t)

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

func CheckSortedFile(t *testing.T, tempFile *common.TemporaryStorage) {
	scanner, err := tempFile.Scanner()
	FatalOnError(err, t)

	lastReviewCount := -1

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		lineDes, err := business.NamedReviewCounterDeserialize(&d)
		FatalOnError(err, t)

		if lastReviewCount == -1 {
			lastReviewCount = int(lineDes.Count)
		} else if int(lineDes.Count) < lastReviewCount {
			t.Fatalf("Expected increasing order, got %d", lineDes.Count)
		}
	}

	t.Log("File sorted correctly")
}

func TestQ5PartialSort(t *testing.T) {
	batch := CreateRandomBatch(100, 0)

	tempFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", fmt.Sprintf("batch_%d", batch.Index)))
	FatalOnError(err, t)

	err = business.Q5PartialSort(batch, tempFile)
	FatalOnError(err, t)

	CheckSortedFile(t, tempFile)
}

func TestQ5MergeSort(t *testing.T) {
	reviewsLen := 0

	tempSortedFiles := []*common.TemporaryStorage{}

	for i := 0; i < 10; i++ {
		batch := CreateRandomBatch(100, i)
		reviewsLen += batch.Size

		tempFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", fmt.Sprintf("batch_%d", batch.Index)))
		FatalOnError(err, t)
		tempFile.Overwrite([]byte{})

		err = business.Q5PartialSort(batch, tempFile)
		FatalOnError(err, t)

		tempSortedFiles = append(tempSortedFiles, tempFile)
	}

	sortedFile, err := common.NewTemporaryStorage(filepath.Join(".", "temp", "query_five", "99", "merge_sort"))
	FatalOnError(err, t)
	sortedFile.Overwrite([]byte{})

	reviewsLenResult, err := business.Q5MergeSort(sortedFile, tempSortedFiles)
	FatalOnError(err, t)

	if reviewsLen != reviewsLenResult {
		t.Fatalf("Expected %d reviews, got %d", reviewsLen, reviewsLenResult)
	} else {
		t.Log("Merge sort done correctly")
	}

	CheckSortedFile(t, sortedFile)
}

func TestQ5CalculateP90(t *testing.T) {
	q5, err := business.NewQ5("temp", "99", 90, 10)
	FatalOnError(err, t)

	q5.Storage.Overwrite([]byte{})

	for i := 100; i > 0; i-- {
		q5.Insert(&business.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
		})
	}

	idx, err := q5.Q5Quantile()
	FatalOnError(err, t)

	if idx != 90 {
		t.Fatalf("Expected index 90, got %d", idx)
	} else {
		t.Log("P90 calculated correctly")
	}
}
