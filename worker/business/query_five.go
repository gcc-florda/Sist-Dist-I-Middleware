package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
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

func (q *Q5) Q5Quantile() (int, error) {
	batch := NewNamedReviewBatch(q.Base, q.JobId, 0)
	tempSortedFiles := []*common.TemporaryStorage{}

	scanner, err := q.Storage.Scanner()
	common.FailOnError(err, "Cannot create scanner")

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		gameReview, err := NamedReviewCounterDeserialize(&d)
		common.FailOnError(err, "Cannot deserialize review")

		if !batch.CanHandle(gameReview) {
			tempSortedFile, err := SortBatch(batch, q.Base, q.JobId)
			if err != nil {
				return 0, err
			}
			tempSortedFiles = append(tempSortedFiles, tempSortedFile)
			batch = NewNamedReviewBatch(q.Base, q.JobId, batch.Index+1)
		} else {
			batch.Add(gameReview)
		}
	}

	// Check if the last batch has any remaining elements
	if batch.Size > 0 {
		tempSortedFile, err := SortBatch(batch, q.Base, q.JobId)
		if err != nil {
			return 0, err
		}
		tempSortedFiles = append(tempSortedFiles, tempSortedFile)
	}

	sortedFile, err := common.NewTemporaryStorage(filepath.Join(".", q.Base, "query_five", q.JobId, "merge_sort"))
	common.FailOnError(err, "Cannot create temporary storage")

	reviewsLen, err := Q5MergeSort(sortedFile, tempSortedFiles)
	common.FailOnError(err, "Cannot merge batches")
	defer sortedFile.Close()

	index := Q5CalculatePi(reviewsLen, q.state.PercentileOver)
	common.FailOnError(err, "Cannot calculate percentile")

	log.Printf("Percentile %d: %d\n", q.state.PercentileOver, index)

	return index, nil
}

func SortBatch(batch *NamedReviewBatch, base string, jobId string) (*common.TemporaryStorage, error) {
	tempSortedFile, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_five", jobId, fmt.Sprintf("batch_%d", batch.Index)))

	if err != nil {
		return nil, err
	}

	err = Q5PartialSort(batch, tempSortedFile)
	if err != nil {
		return nil, err
	}

	return tempSortedFile, nil
}

func Q5PartialSort(batch *NamedReviewBatch, batchTempFile *common.TemporaryStorage) error {
	sort.Slice(batch.Reviews, func(i, j int) bool {
		return batch.Reviews[i].Count < batch.Reviews[j].Count
	})

	for _, review := range batch.Reviews {
		_, err := batchTempFile.AppendLine(review.Serialize())
		if err != nil {
			return err
		}
	}

	return nil
}

func OpenAll(tempSortedFiles []*common.TemporaryStorage) ([]*bufio.Scanner, error) {
	readers := make([]*bufio.Scanner, len(tempSortedFiles))
	for i, tempFile := range tempSortedFiles {
		tempFile.Reset()
		scanner, err := tempFile.Scanner()
		if err != nil {
			return nil, err
		}
		readers[i] = scanner
	}
	return readers, nil
}

func PushHeapAtIdx(h *MinHeap, reader *bufio.Scanner, idx int) error {
	if reader.Scan() {
		d := common.NewDeserializer(reader.Bytes())
		review, err := NamedReviewCounterDeserialize(&d)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		heap.Push(h, ReviewWithSource{
			Review: *review,
			Index:  idx,
		})
	}
	return nil
}

func Q5MergeSort(sortedFile *common.TemporaryStorage, tempSortedFiles []*common.TemporaryStorage) (int, error) {
	readers, err := OpenAll(tempSortedFiles)
	if err != nil {
		return 0, err
	}

	h := NewHeap()

	for i, reader := range readers {
		PushHeapAtIdx(h, reader, i) // initialize the heap with the first element of every temp batch file
	}

	reviewsLen := h.Len()

	for h.Len() > 0 {
		min := heap.Pop(h).(ReviewWithSource)

		_, err := sortedFile.AppendLine(min.Review.Serialize())
		if err != nil {
			return 0, err
		}

		reviewsLen++

		PushHeapAtIdx(h, readers[min.Index], min.Index)
	}

	return reviewsLen, nil
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
	Base    string
	JobId   string
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
		Base:    base,
		JobId:   id,
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
