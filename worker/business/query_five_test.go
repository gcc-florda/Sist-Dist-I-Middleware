package business_test

import (
	"fmt"
	"middleware/common"
	"middleware/worker/business"
	"testing"
)

func FatalOnError(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
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

func TestQ5PartialSort(t *testing.T) {
	batch := business.NewNamedReviewBatch("temp", "99", 0)

	for i := 100; i > 0; i-- {
		batch.Add(&business.NamedReviewCounter{
			Name:  fmt.Sprintf("Game N° %d", i),
			Count: uint32(i),
		})
	}

	tempFile, err := business.Q5PartialSort(batch)
	FatalOnError(err, t)

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

	t.Log("Partial sort done correctly")
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
