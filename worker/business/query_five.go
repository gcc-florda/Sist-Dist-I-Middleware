package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"middleware/common"
	"os"
	"sort"

	"path/filepath"
	"reflect"
)

// Batch size in bytes (34MB)
const maxBatchSize = 34 * 1024 * 1024

func Q5FilterGames(r *Game, cat string) bool {
	return common.Contains(r.Categories, cat)
}

func Q5FilterReviews(r *Review, pos bool) bool {
	if pos {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q5MapGames(r *Game) *GameName {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q5MapReviews(r *Review) *ValidReview {
	return &ValidReview{
		AppID: r.AppID,
	}
}

func (q *Q5) Q5Quantile() error {
	batch := []*NamedReviewCounter{}
	currentBatchSize := 0
	var tempFiles []string

	scanner, err := q.storage.Scanner()
	common.FailOnError(err, "Cannot create scanner")

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		gameReview, err := NamedReviewCounterDeserialize(&d)
		common.FailOnError(err, "Cannot deserialize review")
		batch = append(batch, gameReview)
		currentBatchSize += len(line)

		if currentBatchSize >= maxBatchSize {
			tempFile, err := ProcessAndStoreBatch(batch)
			common.FailOnError(err, "Cannot process and store batch")
			tempFiles = append(tempFiles, tempFile)

			batch = []*NamedReviewCounter{}
			currentBatchSize = 0
		}
	}

	// Check if the last batch has any remaining elements
	if len(batch) > 0 {
		tempFile, err := ProcessAndStoreBatch(batch)
		common.FailOnError(err, "Cannot process and store batch")
		tempFiles = append(tempFiles, tempFile)
	}

	_, reviewsLen, err := MergeBatches(tempFiles)
	common.FailOnError(err, "Cannot merge batches")

	index := CalculatePercentile(reviewsLen, q.state.PercentileOver)
	common.FailOnError(err, "Cannot calculate percentile")

	log.Printf("Percentile %d: %d\n", q.state.PercentileOver, index)

	return nil
}

func ProcessAndStoreBatch(batch []*NamedReviewCounter) (string, error) {
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].Count < batch[j].Count
	})

	tempFile, err := os.CreateTemp("", "batch_*.txt")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	for _, review := range batch {
		_, err := writer.WriteString(fmt.Sprintf("%s %d\n", review.Name, review.Count))
		if err != nil {
			return "", err
		}
	}
	writer.Flush()

	return tempFile.Name(), nil
}

func MergeBatches(tempFiles []string) (string, int, error) {
	// Open all the temp batch files
	readers := make([]*bufio.Scanner, len(tempFiles))
	files := make([]*os.File, len(tempFiles))
	for i, tempFile := range tempFiles {
		file, err := os.Open(tempFile)
		if err != nil {
			return "", 0, err
		}
		files[i] = file
		readers[i] = bufio.NewScanner(file)
	}

	// Create the file where the final sorted data will be stored
	outputFile, err := os.CreateTemp("", "final_sorted.txt")
	if err != nil {
		return "", 0, err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)

	h := &MinHeap{}
	heap.Init(h)

	// Initialize the heap with the first line of every temp batch file
	for i, reader := range readers {
		if reader.Scan() {
			d := common.NewDeserializer(reader.Bytes())
			review, err := NamedReviewCounterDeserialize(&d)
			common.FailOnError(err, "Cannot deserialize review")
			heap.Push(h, ReviewWithSource{
				Review: *review,
				Index:  i,
			})
		}
	}

	reviewsLen := h.Len()

	for h.Len() > 0 {
		min := heap.Pop(h).(ReviewWithSource)

		_, err := writer.WriteString(fmt.Sprintf("%s %d\n", min.Review.Name, min.Review.Count))
		if err != nil {
			return "", 0, err
		}

		reviewsLen++

		// Read the next record from the file where the minimum came from
		if readers[min.Index].Scan() {
			d := common.NewDeserializer(readers[min.Index].Bytes())
			nextReview, err := NamedReviewCounterDeserialize(&d)
			common.FailOnError(err, "Cannot deserialize review")
			heap.Push(h, ReviewWithSource{
				Review: *nextReview,
				Index:  min.Index,
			})
		}
	}

	writer.Flush()

	for _, file := range files {
		file.Close()
	}

	return outputFile.Name(), reviewsLen, nil
}

func CalculatePercentile(reviewsLen int, percentile uint32) int {

	index := int(float64(reviewsLen) * (float64(percentile) / 100.0))

	return index
}

type Q5State struct {
	PercentileOver uint32
	bufSize        int
}

type Q5 struct {
	state   *Q5State
	storage *common.TemporaryStorage
}

func NewQ5(base string, id string, pctOver int, bufSize int) (*Q5, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_five", id, "results"))
	if err != nil {
		return nil, err
	}

	return &Q5{
		state: &Q5State{
			PercentileOver: uint32(pctOver),
			bufSize:        bufSize,
		},
		storage: s,
	}, nil
}

func (q *Q5) Insert(rc *NamedReviewCounter) error {
	_, err := q.storage.Append(rc.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func (q *Q5) NextStage() (chan *NamedReviewCounter, chan error) {
	cr := make(chan *NamedReviewCounter, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		// q.storage.Reset()

		// s, err := q.storage.Scanner()
		// if err != nil {
		// 	ce <- err
		// 	return
		// }

		// for s.Scan() {
		// 	b := s.Bytes()
		// 	d := common.NewDeserializer(b)
		// 	nrc, err := NamedReviewCounterDeserialize(&d)
		// 	if err != nil {
		// 		ce <- err
		// 		return
		// 	}

		// 	cr <- nrc
		// }

		// if err := s.Err(); err != nil {
		// 	ce <- err
		// 	return
		// }
	}()

	return cr, ce
}

func (q *Q5) Handle(protocolData []byte) error {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&NamedReviewCounter{}) {
		return q.Insert(p.(*NamedReviewCounter))
	}
	return &UnknownTypeError{}
}

func (q *Q5) Shutdown() {
	q.storage.Close()
}
