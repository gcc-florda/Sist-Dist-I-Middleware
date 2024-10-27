package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"os"

	"path/filepath"
	"reflect"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Batch size in bytes (34MB)
const maxBatchSize = 34 * 1024 * 1024

func Q5FilterGames(r *Game) bool {
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.five.category"))
}

func Q5FilterReviews(r *Review) bool {
	if common.Config.GetBool("query.five.positive") {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q5MapGames(r *Game) controller.Partitionable {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q5MapReviews(r *Review) controller.Partitionable {
	return &ValidReview{
		AppID: r.AppID,
	}
}

func (q *Q5) Q5Quantile() (int, error) {
	defer q.deleteTemp()
	batch := NewNamedReviewBatch(q.Base, q.JobId, 0)
	tempSortedFiles := make([]*common.TemporaryStorage, 0)

	scanner, err := q.Storage.Scanner()
	q.Storage.Reset()
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
			defer tempSortedFile.Close()
			tempSortedFiles = append(tempSortedFiles, tempSortedFile)
			batch = NewNamedReviewBatch(q.Base, q.JobId, batch.Index+1)
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

func SortBatch(batch *NamedReviewBatch, base string, jobId string) (*common.TemporaryStorage, error) {
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

func Q5PartialSort(batch *NamedReviewBatch, batchTempFile *common.TemporaryStorage) error {
	h := NewHeap()

	for _, review := range batch.Reviews {
		heap.Push(h, ReviewWithSource{
			Review: *review,
			Index:  0, // unused here
		})
	}

	for h.Len() > 0 {
		minReview := heap.Pop(h).(ReviewWithSource)
		_, err := batchTempFile.AppendLine(minReview.Review.Serialize())
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
		nrc, err := NamedReviewCounterDeserialize(&d)
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

		bytesWritten, err := sortedFile.AppendLine(min.Review.Serialize())
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
	_, err := q.Storage.AppendLine(rc.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func (q *Q5) NextStage() (<-chan controller.Partitionable, <-chan error) {
	cr := make(chan controller.Partitionable, q.state.bufSize)
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

		s, err := q.sortedStorage.Scanner()

		if err != nil {
			ce <- err
			return
		}
		i := 0
		for s.Scan() {
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := NamedReviewCounterDeserialize(&d)
			if err != nil {
				ce <- err
				return
			}

			cr <- nrc
			i++
			if i >= idx {
				return
			}
		}

		if err := s.Err(); err != nil {
			ce <- err
			return
		}
	}()

	return cr, ce
}

func (q *Q5) Handle(protocolData []byte) (controller.Partitionable, error) {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&NamedReviewCounter{}) {
		return nil, q.Insert(p.(*NamedReviewCounter))
	}
	return nil, &UnknownTypeError{}
}

func (q *Q5) Shutdown(delete bool) {
	q.Storage.Close()
	if delete {
		err := q.Storage.Delete()
		if err != nil {
			log.Errorf("Error while deleting the file: %s", err)
		}
	}
}
