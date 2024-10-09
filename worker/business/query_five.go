package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"middleware/common"
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

type Q5NamedReviewBatch struct {
	Reviews []*NamedReviewCounter
	Size    int
	Base    string
	JobId   string
	Index   int
}

func NewNamedReviewBatch(base string, jobId string, index int) *Q5NamedReviewBatch {
	return &Q5NamedReviewBatch{
		Reviews: []*NamedReviewCounter{},
		Size:    0,
		Base:    base,
		JobId:   jobId,
		Index:   index,
	}
}

func (b *Q5NamedReviewBatch) CanHandle(r *NamedReviewCounter) bool {
	return b.Size+len(r.Serialize()) < maxBatchSize
}

func (b *Q5NamedReviewBatch) Add(r *NamedReviewCounter) {
	b.Reviews = append(b.Reviews, r)
	b.Size += len(r.Serialize())
}

func (q *Q5) Q5Quantile() (int, error) {
	batch := NewNamedReviewBatch(q.base, q.id, 0)
	tempSortedFiles := []*common.TemporaryStorage{}

	scanner, err := q.Storage.Scanner()
	common.FailOnError(err, "Cannot create scanner")

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		gameReview, err := NamedReviewCounterDeserialize(&d)
		common.FailOnError(err, "Cannot deserialize review")

		if !batch.CanHandle(gameReview) {
			tempFile, err := Q5PartialSort(batch)
			common.FailOnError(err, "Cannot process and store batch")
			tempSortedFiles = append(tempSortedFiles, tempFile)

			batch = NewNamedReviewBatch(q.base, q.id, batch.Index+1)
		} else {
			batch.Add(gameReview)
		}
	}

	// Check if the last batch has any remaining elements
	if batch.Size > 0 {
		tempFile, err := Q5PartialSort(batch)
		common.FailOnError(err, "Cannot process and store batch")
		tempSortedFiles = append(tempSortedFiles, tempFile)
	}

	outputFile, reviewsLen, err := Q5MergeSort(tempSortedFiles, q.base, q.id)
	common.FailOnError(err, "Cannot merge batches")
	defer outputFile.Close()

	index := Q5CalculatePi(reviewsLen, q.state.PercentileOver)
	common.FailOnError(err, "Cannot calculate percentile")

	log.Printf("Percentile %d: %d\n", q.state.PercentileOver, index)

	return index, nil
}

func Q5PartialSort(batch *Q5NamedReviewBatch) (*common.TemporaryStorage, error) {
	sort.Slice(batch.Reviews, func(i, j int) bool {
		return batch.Reviews[i].Count < batch.Reviews[j].Count
	})

	batchTempFile, err := common.NewTemporaryStorage(filepath.Join(".", batch.Base, "query_five", batch.JobId, fmt.Sprintf("batch_%d", batch.Index)))
	if err != nil {
		return nil, err
	}

	for _, review := range batch.Reviews {
		batchTempFile.AppendLine(review.Serialize())
		if err != nil {
			return nil, err
		}
	}

	return batchTempFile, nil
}

func Q5MergeSort(tempFiles []*common.TemporaryStorage, base string, jobId string) (*common.TemporaryStorage, int, error) {
	// Open all the temp batch files
	readers := make([]*bufio.Scanner, len(tempFiles))
	for i, tempFile := range tempFiles {
		scanner, err := tempFile.Scanner()
		if err != nil {
			return nil, 0, err
		}
		readers[i] = scanner
	}

	// Create the file where the final sorted data will be stored
	outputFile, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_five", jobId, "merge_sort"))
	if err != nil {
		return nil, 0, err
	}

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

		_, err := outputFile.AppendLine(min.Review.Serialize())
		if err != nil {
			return nil, 0, err
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

	return outputFile, reviewsLen, nil
}

func Q5CalculatePi(reviewsLen int, percentile uint32) int {
	return int(float64(reviewsLen) * (float64(percentile) / 100.0))
}

type Q5State struct {
	PercentileOver uint32
	bufSize        int
}

type Q5 struct {
	state   *Q5State
	base    string
	id      string
	Storage *common.TemporaryStorage
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
		base:    base,
		id:      id,
		Storage: s,
	}, nil
}

func (q *Q5) Insert(rc *NamedReviewCounter) error {
	_, err := q.Storage.Append(rc.Serialize())
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
	q.Storage.Close()
}
