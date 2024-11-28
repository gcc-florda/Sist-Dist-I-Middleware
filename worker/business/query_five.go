package business

import (
	"bufio"
	"container/heap"
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"os"
	"sort"

	"path/filepath"
	"reflect"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Batch size in bytes (34MB)
const maxBatchSize = 34 * 1024 * 1024

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

type SortedBatch[T common.Serializable] struct {
	Items     []T
	Size      int
	Weight    int
	MaxWeight int
	SaveTo    string
}

func NewSortedBatch[T common.Serializable](maxWeight int, savePath string) *SortedBatch[T] {
	return &SortedBatch[T]{
		Items:     make([]T, 0),
		Size:      0,
		Weight:    0,
		MaxWeight: maxWeight,
		SaveTo:    savePath,
	}
}

func (b *SortedBatch[T]) Save(less func(T, T) bool) error {
	stg, err := common.NewTemporaryStorage(b.SaveTo)
	if err != nil {
		return err
	}
	defer stg.Close()
	sort.Slice(b.Items, func(i, j int) bool {
		return less(b.Items[i], b.Items[j])
	})

	by := make([]byte, 0, b.Size)
	for _, item := range b.Items {
		by = append(by, item.Serialize()...)
	}
	// Avoid Syncing for every line, this is redundant, temporary data, we can afford to lose it
	_, err = stg.Append(by)
	if err != nil {
		stg.Delete()
		return err
	}
	return nil
}

func (b *SortedBatch[T]) CanHandle(r *schema.NamedReviewCounter) bool {
	return b.Weight+len(r.Serialize()) < maxBatchSize
}

func (b *SortedBatch[T]) Add(r T) {
	b.Items = append(b.Items, r)
	b.Size++
	b.Weight += len(r.Serialize())
}

type NamedReviewCounterBatchManager struct {
	batches   []*SortedBatch[*schema.NamedReviewCounter]
	currBatch *SortedBatch[*schema.NamedReviewCounter]
	base      string
	openFiles []*common.TemporaryStorage
}

func NewNamedReviewCounterBatchManager(base string) *NamedReviewCounterBatchManager {
	b := make([]*SortedBatch[*schema.NamedReviewCounter], 0)
	batch := NewSortedBatch[*schema.NamedReviewCounter](maxBatchSize, filepath.Join(base, fmt.Sprintf("batch_%d", 0)))

	b = append(b, batch)
	return &NamedReviewCounterBatchManager{
		batches:   b,
		currBatch: batch,
		base:      base,
		openFiles: make([]*common.TemporaryStorage, 0),
	}
}

func (m *NamedReviewCounterBatchManager) saveCurrentBatch() error {
	return m.currBatch.Save(func(x *schema.NamedReviewCounter, y *schema.NamedReviewCounter) bool {
		return x.Count < y.Count
	})
}

func (m *NamedReviewCounterBatchManager) nextBatch() {
	batch := NewSortedBatch[*schema.NamedReviewCounter](maxBatchSize, filepath.Join(m.base, fmt.Sprintf("batch_%d", len(m.batches))))
	m.currBatch = batch
	m.batches = append(m.batches, batch)
}

func (m *NamedReviewCounterBatchManager) Add(r *schema.NamedReviewCounter) error {
	if !m.currBatch.CanHandle(r) {
		err := m.saveCurrentBatch()
		if err != nil {
			return err
		}
		m.nextBatch()
	}
	m.currBatch.Add(r)
	return nil
}

func (m *NamedReviewCounterBatchManager) End() error {
	if m.currBatch.Size > 0 {
		return m.saveCurrentBatch()
	}
	return nil
}

func (m *NamedReviewCounterBatchManager) FileHandlers() ([]*common.TemporaryStorage, error) {
	var errr error = nil
	files := make([]*common.TemporaryStorage, len(m.batches))
	for i, b := range m.batches {
		stg, err := common.NewTemporaryStorage(b.SaveTo)
		if err != nil {
			errr = err
			break
		}
		files[i] = stg
	}
	if errr != nil {
		for _, stg := range files {
			stg.Close()
		}
		return nil, errr
	}
	m.openFiles = append(m.openFiles, files...)
	return files, nil
}

func (m *NamedReviewCounterBatchManager) Cleanup() {
	for _, stg := range m.openFiles {
		stg.Close()
	}
	os.RemoveAll(m.base)
}

type Q5State struct {
	PercentileOver uint32
	bufSize        int
}

type Q5 struct {
	state         *Q5State
	Base          string
	JobId         string
	Storage       *common.IdempotencyHandlerSingleFile[*schema.NamedReviewCounter]
	sortedStorage *common.TemporaryStorage
	basefiles     string
}

func NewQ5(base string, id string, partition int, pctOver int, bufSize int) (*Q5, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("query_five_%d", partition), id)
	s, err := common.NewIdempotencyHandlerSingleFile[*schema.NamedReviewCounter](filepath.Join(basefiles, "results"))
	if err != nil {
		return nil, err
	}

	_, err = s.LoadOverwriteState(schema.NamedReviewCounterDeserialize)
	if err != nil {
		return nil, err
	}

	return &Q5{
		state: &Q5State{
			PercentileOver: uint32(pctOver),
			bufSize:        bufSize,
		},
		Base:      base,
		JobId:     id,
		Storage:   s,
		basefiles: basefiles,
	}, nil
}

func (q *Q5) Q5Quantile() (int, error) {
	sortedResultFileName := filepath.Join(q.basefiles, "results.sorted")
	os.Remove(sortedResultFileName)
	batchManager := NewNamedReviewCounterBatchManager(filepath.Join(q.basefiles, "sort_temporary"))
	defer batchManager.Cleanup()

	nrcs, err := q.Storage.ReadState(schema.NamedReviewCounterDeserialize)
	if err != nil {
		return 0, err
	}

	for nrc := range nrcs {
		if err = batchManager.Add(nrc); err != nil {
			return 0, err
		}
	}
	if err = batchManager.End(); err != nil {
		return 0, err
	}

	sortedFile, err := common.NewTemporaryStorage(sortedResultFileName)
	if err != nil {
		return 0, err
	}
	q.sortedStorage = sortedFile

	partialSortHandlers, err := batchManager.FileHandlers()
	if err != nil {
		return 0, err
	}

	reviewsLen, err := Q5MergeSort(sortedFile, partialSortHandlers)
	if err != nil {
		return 0, err
	}

	index := Q5CalculatePi(reviewsLen, q.state.PercentileOver)

	log.Debugf("Percentile %d: %d\n", q.state.PercentileOver, index)

	return index, nil
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

	to_write := make([]byte, 0, maxBatchSize)

	for h.Len() > 0 {
		min := heap.Pop(h).(ReviewWithSource)

		to_write = append(to_write, min.Review.Serialize()...)
		if len(to_write) >= maxBatchSize {
			// Avoid Syncing every line, this is redundant data that we can recalculate
			bytesWritten, err := sortedFile.Append(min.Review.Serialize())
			if err != nil || bytesWritten == 0 {
				return 0, err
			}
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

func (q *Q5) Insert(rc *schema.NamedReviewCounter, idempotencyID *common.IdempotencyID) error {
	if rc.Count <= 0 {
		// TODO: We are not really storing these IDs, but do we really care?
		// They are not modifying state, so they can come as much as they want.
		return nil
	}
	err := q.Storage.SaveState(idempotencyID, rc)
	if err != nil {
		return err
	}

	return nil
}

func (q *Q5) sortedStorageScanner() (*bufio.Scanner, error) {
	q.sortedStorage.Reset()
	return q.sortedStorage.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := schema.NamedReviewCounterDeserialize(d)
		return err
	})
}

func (q *Q5) getQuantileVal(at_idx int) (uint32, error) {
	if q.sortedStorage == nil {
		log.Fatalf("action: Q5::quantile::get_value | status: fatal | error: the sorted storage is nil")
	}
	s, err := q.sortedStorageScanner()
	if err != nil {
		return 0, err
	}
	var val uint32

	i := 0
	for s.Scan() {
		if i == at_idx {
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				return 0, err
			}
			val = nrc.Count
			break
		}
		i++
	}
	return val, nil
}

func (q *Q5) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	cr := make(chan *controller.NextStageMessage, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		idx, err := q.Q5Quantile()
		if err != nil {
			ce <- err
			return
		}

		val, err := q.getQuantileVal(idx)
		if err != nil {
			ce <- err
			return
		}

		s, err := q.sortedStorageScanner()
		if err != nil {
			ce <- err
			return
		}

		fs, err := NewFileSequence(filepath.Join(q.basefiles, "sent_lines"))
		if err != nil {
			ce <- err
			return
		}
		var line uint32 = 1
		for s.Scan() {
			if line < fs.LastSent() {
				continue
			}
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				ce <- err
				return
			}
			if nrc.Count >= val {
				cr <- &controller.NextStageMessage{
					Message:      nrc,
					Sequence:     line,
					SentCallback: fs.Sent,
				}
			}
			line++
		}

		cr <- &controller.NextStageMessage{
			Message:      nil,
			Sequence:     line + 1,
			SentCallback: nil,
		}

		if err := s.Err(); err != nil {
			ce <- err
			return
		}
	}()

	return cr, ce
}

func (q *Q5) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if q.Storage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Game Percentile %d | Result: Already processed | IdempotencyID: %s", q.state.PercentileOver, idempotencyID)
		return nil, nil
	}

	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.NamedReviewCounter{}) {
		return nil, q.Insert(p.(*schema.NamedReviewCounter), idempotencyID)
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
