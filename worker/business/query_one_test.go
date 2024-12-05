package business_test

import (
	"middleware/common"
	"middleware/worker/business"
	"middleware/worker/schema"
	"os"
	"path/filepath"
	"testing"
)

func init() {
	os.RemoveAll(filepath.Join(".", "test_files"))
}

func boolToNr(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func TestQueryOneEnd2End(t *testing.T) {
	q1, _ := business.NewQ1(filepath.Join(".", "test_files"), "query_one", 1, "test_normal")

	for i := 0; i < 100; i++ {

		b := (&schema.SOCounter{
			AppId:   "a",
			Windows: 1,
			Linux:   boolToNr(i%2 == 0),
			Mac:     boolToNr(i%3 == 0),
		}).Serialize()
		b1 := make([]byte, 1, len(b)+1)
		b1[0] = common.Type_SOCounter
		b1 = append(b1, b...)
		q1.Handle(b1, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(i + 1),
		})
	}

	cr, _ := q1.NextStage()

	for r := range cr {
		d := common.NewDeserializer(r.Message.Serialize())
		c, _ := schema.SOCounterDeserialize(&d)
		if c.Windows != 100 {
			t.Fatalf("Wrong windows amounts")
		}

		if c.Linux != 50 {
			t.Fatalf("Wrong Linux amounts")
		}

		if c.Mac != 34 {
			t.Fatalf("Wrong Mac amounts")
		}
	}
}

func TestQueryOneLoadData(t *testing.T) {
	q1, _ := business.NewQ1(filepath.Join(".", "test_files"), "query_one", 1, "test_interrupt")

	for i := 0; i < 100; i++ {

		b := (&schema.SOCounter{
			AppId:   "a",
			Windows: 1,
			Linux:   boolToNr(i%2 == 0),
			Mac:     boolToNr(i%3 == 0),
		}).Serialize()
		b1 := make([]byte, 1, len(b)+1)
		b1[0] = common.Type_SOCounter
		b1 = append(b1, b...)
		q1.Handle(b1, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(i + 1),
		})
	}

	q1.Shutdown(false)

	q12, _ := business.NewQ1(filepath.Join(".", "test_files"), "test_normal", 1, "test")

	cr, _ := q12.NextStage()

	for r := range cr {
		d := common.NewDeserializer(r.Message.Serialize())
		c, _ := schema.SOCounterDeserialize(&d)
		if c.Windows != 100 {
			t.Fatalf("Wrong windows amounts")
		}

		if c.Linux != 50 {
			t.Fatalf("Wrong Linux amounts")
		}

		if c.Mac != 34 {
			t.Fatalf("Wrong Mac amounts")
		}
	}
}

func TestQueryOneLoadDataRepeated(t *testing.T) {
	q1, _ := business.NewQ1(filepath.Join(".", "test_files"), "query_one", 1, "test_interrupt_repeated")

	for i := 0; i < 100; i++ {

		b := (&schema.SOCounter{
			AppId:   "a",
			Windows: 1,
			Linux:   boolToNr(i%2 == 0),
			Mac:     boolToNr(i%3 == 0),
		}).Serialize()
		b1 := make([]byte, 1, len(b)+1)
		b1[0] = common.Type_SOCounter
		b1 = append(b1, b...)
		q1.Handle(b1, &common.IdempotencyID{
			Origin:   "A",
			Sequence: uint32(i + 1),
		})

		if i%4 == 0 {
			q1.Handle(b1, &common.IdempotencyID{
				Origin:   "A",
				Sequence: uint32(i + 1),
			})
		}
	}

	q1.Shutdown(false)

	q12, _ := business.NewQ1(filepath.Join(".", "test_files"), "query_one", 1, "test_interrupt_repeated")

	cr, _ := q12.NextStage()

	for r := range cr {
		d := common.NewDeserializer(r.Message.Serialize())
		c, _ := schema.SOCounterDeserialize(&d)
		if c.Windows != 100 {
			t.Fatalf("Wrong windows amounts")
		}

		if c.Linux != 50 {
			t.Fatalf("Wrong Linux amounts")
		}

		if c.Mac != 34 {
			t.Fatalf("Wrong Mac amounts")
		}
	}
}
