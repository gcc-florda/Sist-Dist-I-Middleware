package common

type Batch struct {
	data   []string
	size   int
	weight int
}

func (b *Batch) AppendData(line string) {
	b.data = append(b.data, line)
	b.size++
	b.weight += len(line)
}

func (b *Batch) Serialize() string {
	var res string
	for _, line := range b.data {
		res += line
	}
	return res
}

func (b *Batch) CanHandle(line string, batchMaxAmount int) bool {
	return !(b.size+1 > batchMaxAmount || float64(b.weight+len(line))/1024.0 > 8)
}

func (b *Batch) IsFull(batchMaxAmount int) bool {
	return b.size+1 > batchMaxAmount || float64(b.weight)/1024.0 > 8
}

func (b *Batch) Size() int {
	return b.size
}

func NewBatch() Batch {
	return Batch{
		data:   []string{},
		size:   0,
		weight: 0,
	}
}
