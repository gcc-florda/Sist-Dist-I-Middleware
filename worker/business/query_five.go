package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"middleware/common"
	"middleware/worker/schema"
	"os"

	"path/filepath"
	"reflect"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Batch size in bytes (34MB)

func Q5FilterGames(r *schema.Game) bool {
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.five.category"))
}

func Q5FilterReviews(r *schema.Review) bool {
	if common.Config.GetBool("query.five.positive") {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q5MapGames(r *schema.Game) schema.Partitionable {
	return &schema.GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q5MapReviews(r *schema.Review) schema.Partitionable {
	return &schema.ValidReview{
		AppID: r.AppID,
	}
}

func (q *Q5) Q5Quantile() (int, error) {
	defer q.deleteTemp()
	batch := schema.NewNamedReviewBatch(q.Base, q.JobId, 0)
	tempSortedFiles := make([]*common.TemporaryStorage, 0)

	scanner, err := q.Storage.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := schema.NamedReviewCounterDeserialize(d)
		return err
	})
	q.Storage.Reset()
	common.FailOnError(err, "Cannot create scanner")

	for scanner.Scan() {
		line := scanner.Bytes()

		d := common.NewDeserializer(line)
		gameReview, err := schema.NamedReviewCounterDeserialize(&d)
		common.FailOnError(err, "Cannot deserialize review")

		if !batch.CanHandle(gameReview) {
			tempSortedFile, err := SortBatch(batch, q.Base, q.JobId)
			if err != nil {
				return 0, err
			}
			defer tempSortedFile.Close()
			tempSortedFiles = append(tempSortedFiles, tempSortedFile)
			batch = schema.NewNamedReviewBatch(q.Base, q.JobId, batch.Index+1)
		}
		batch.Add(gameReview)
	}

	// Check if the last batch has any remaining elements
	if batch.Size > 0 {
		tempSortedFile, err := SortBatch(batch, q.Base, q.JobId)
		if err != nil {
			return 0, err
		}
		defer tempSortedFile.Close()
		tempSortedFiles = append(tempSortedFiles, tempSortedFile)
	}

	sortedFile, err := common.NewTemporaryStorage(filepath.Join(".", q.Base, "query_five", q.JobId, "results.sorted"))
	common.FailOnError(err, "Cannot create temporary storage")
	q.sortedStorage = sortedFile

	reviewsLen, err := Q5MergeSort(sortedFile, tempSortedFiles)
	common.FailOnError(err, "Cannot merge batches")

	index := Q5CalculatePi(reviewsLen, q.state.PercentileOver)
	common.FailOnError(err, "Cannot calculate percentile")

	log.Debugf("Percentile %d: %d\n", q.state.PercentileOver, index)

	return index, nil
}

func (q *Q5) deleteTemp() {
	d := filepath.Join(".", q.Base, "query_five", q.JobId, "temp")
	err := os.RemoveAll(d)
	if err != nil {
		log.Errorf("Failed to delete temp directory: %v", err)
	}
}

func SortBatch(batch *schema.NamedReviewBatch, base string, jobId string) (*common.TemporaryStorage, error) {
	tempSortedFile, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_five", jobId, "temp", fmt.Sprintf("batch_%d", batch.Index)))

	if err != nil {
		return nil, err
	}

	err = Q5PartialSort(batch, tempSortedFile)
	if err != nil {
		return nil, err
	}

	return tempSortedFile, nil
}

func Q5PartialSort(batch *schema.NamedReviewBatch, batchTempFile *common.TemporaryStorage) error {
	h := NewHeap()

	for _, review := range batch.Reviews {
		heap.Push(h, ReviewWithSource{
			Review: *review,
			Index:  0, // unused here
		})
	}

	for h.Len() > 0 {
		minReview := heap.Pop(h).(ReviewWithSource)
		_, err := batchTempFile.Append(minReview.Review.Serialize())
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
		scanner, err := tempFile.ScannerDeserialize(func(d *common.Deserializer) error {
			_, err := schema.NamedReviewCounterDeserialize(d)
			return err
		})
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
		nrc, err := schema.NamedReviewCounterDeserialize(&d)
		if err != nil {
			return err
		}
		heap.Push(h, ReviewWithSource{
			Review: *nrc,
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
		err = PushHeapAtIdx(h, reader, i) // initialize the heap with the first element of every temp batch file
		if err != nil {
			return 0, err
		}
	}

	reviewsLen := 0

	for h.Len() > 0 {
		min := heap.Pop(h).(ReviewWithSource)

		bytesWritten, err := sortedFile.Append(min.Review.Serialize())
		if err != nil || bytesWritten == 0 {
			return 0, err
		}

		reviewsLen++

		err = PushHeapAtIdx(h, readers[min.Index], min.Index)
		if err != nil {
			return 0, err
		}
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
	state         *Q5State
	Base          string
	JobId         string
	Storage       *common.TemporaryStorage
	sortedStorage *common.TemporaryStorage
}

func NewQ5(base string, id string, partition int, pctOver int, bufSize int) (*Q5, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, fmt.Sprintf("query_five_%d", partition), id, "results"))
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

func (q *Q5) Insert(rc *schema.NamedReviewCounter) error {
	if rc.Count <= 0 {
		return nil
	}
	_, err := q.Storage.Append(rc.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func (q *Q5) NextStage() (<-chan schema.Partitionable, <-chan error) {
	cr := make(chan schema.Partitionable, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		idx, err := q.Q5Quantile()
		if err != nil {
			ce <- err
			return
		}

		q.sortedStorage.Reset()

		s, err := q.sortedStorage.ScannerDeserialize(func(d *common.Deserializer) error {
			_, err = schema.NamedReviewCounterDeserialize(d)
			return err
		})

		if err != nil {
			ce <- err
			return
		}
		i := 0
		for s.Scan() {
			if i < idx {
				i++
				continue
			}
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				ce <- err
				return
			}
			cr <- nrc
			i++
		}
		if err := s.Err(); err != nil {
			ce <- err
			return
		}
	}()

	return cr, ce
}

func (q *Q5) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.NamedReviewCounter{}) {
		return nil, q.Insert(p.(*schema.NamedReviewCounter))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q5) Shutdown(delete bool) {
	q.Storage.Close()
	if delete {
		err := q.Storage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
