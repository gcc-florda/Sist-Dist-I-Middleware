package business_test

import (
	"fmt"
	"math/rand"
	"middleware/common"
	"middleware/worker/business"
	"middleware/worker/schema"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func init() {
	os.RemoveAll(filepath.Join(".", "test_files"))
}

func TestJoinSameOrigins(t *testing.T) {
	ga := 10
	ra := 10
	h, err := business.NewJoin(filepath.Join(".", "test_files", "so"), "qtest", "id", 1, 100)
	if err != nil {
		t.Fatalf("Cant create join: %s", err)
	}

	for i := 0; i < ga; i++ {
		h.AddGame(&schema.GameName{
			AppID: fmt.Sprint(i),
			Name:  fmt.Sprint(i),
		}, &common.IdempotencyID{
			Origin:   "AG",
			Sequence: uint32(i),
		})
	}

	ri := 0
	for j := 0; j < ga; j++ {
		for i := ra - j; i > 0; i-- {
			h.AddReview(&schema.ValidReview{AppID: fmt.Sprint(j)}, &common.IdempotencyID{
				Origin:   "AR",
				Sequence: uint32(ri),
			})
			ri++
		}

	}
	cr, ce := h.NextStage()

	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break
			}
			if r.Message == nil {
				return
			}
			d := common.NewDeserializer(r.Message.Serialize())
			m, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			nc, err := strconv.Atoi(m.Name)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			if int(m.Count) != ga-nc {
				t.Fatalf("Game %s with count %d", m.Name, m.Count)
			}
			if r.SentCallback != nil {
				r.SentCallback()
			}

		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			t.Fatalf("There was an error while making the next stage %s", err)
		}
	}
}

func TestJoinMultipleOrigins(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	ga := 10
	ra := 10
	h, err := business.NewJoin(filepath.Join(".", "test_files", "mo"), "qtest", "id", 1, 100)
	if err != nil {
		t.Fatalf("Cant create join: %s", err)
	}

	for i := 0; i < ga; i++ {
		h.AddGame(&schema.GameName{
			AppID: fmt.Sprint(i),
			Name:  fmt.Sprint(i),
		}, &common.IdempotencyID{
			Origin:   fmt.Sprintf("AG%d", r.Int()),
			Sequence: uint32(i),
		})
	}

	ri := 0
	for j := 0; j < ga; j++ {
		for i := ra - j; i > 0; i-- {
			h.AddReview(&schema.ValidReview{AppID: fmt.Sprint(j)}, &common.IdempotencyID{
				Origin:   fmt.Sprintf("AR%d", r.Int()),
				Sequence: uint32(ri),
			})
			ri++
		}

	}
	cr, ce := h.NextStage()

	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break
			}
			if r.Message == nil {
				return
			}
			d := common.NewDeserializer(r.Message.Serialize())
			m, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			nc, err := strconv.Atoi(m.Name)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			if int(m.Count) != ga-nc {
				t.Fatalf("Game %s with count %d", m.Name, m.Count)
			}
			if r.SentCallback != nil {
				r.SentCallback()
			}

		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			t.Fatalf("There was an error while making the next stage %s", err)
		}
	}
}

func TestJoinMultipleOriginsInterrupted(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	ga := 10
	ra := 10
	h, err := business.NewJoin(filepath.Join(".", "test_files", "moi"), "qtest", "id", 1, 100)
	if err != nil {
		t.Fatalf("Cant create join: %s", err)
	}

	for i := 0; i < ga; i++ {
		h.AddGame(&schema.GameName{
			AppID: fmt.Sprint(i),
			Name:  fmt.Sprint(i),
		}, &common.IdempotencyID{
			Origin:   fmt.Sprintf("AG%d", r.Int()),
			Sequence: uint32(i),
		})
	}

	ri := 0
	for j := 0; j < ga; j++ {
		for i := ra - j; i > 0; i-- {
			h.AddReview(&schema.ValidReview{AppID: fmt.Sprint(j)}, &common.IdempotencyID{
				Origin:   fmt.Sprintf("AR%d", r.Int()),
				Sequence: uint32(ri),
			})
			ri++
		}
	}

	h.Shutdown(false)

	h2, err := business.NewJoin(filepath.Join(".", "test_files", "moi"), "qtest", "id", 1, 100)
	cr, ce := h2.NextStage()

	for {
		select {
		case r, ok := <-cr:
			if !ok {
				break
			}
			if r.Message == nil {
				return
			}
			d := common.NewDeserializer(r.Message.Serialize())
			m, err := schema.NamedReviewCounterDeserialize(&d)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			nc, err := strconv.Atoi(m.Name)
			if err != nil {
				t.Fatalf("There was an error while deserializing a join result %s", err)
			}

			if int(m.Count) != ga-nc {
				t.Fatalf("Game %s with count %d", m.Name, m.Count)
			}
			if r.SentCallback != nil {
				r.SentCallback()
			}

		case err, ok := <-ce:
			if err == nil && !ok {
				continue
			}
			t.Fatalf("There was an error while making the next stage %s", err)
		}
	}
}
