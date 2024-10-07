package business_test

import (
	"middleware/worker/business"
	"testing"

	"github.com/pemistahl/lingua-go"
)

func TestQ4ReviewFilteringEnglish(t *testing.T) {
	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(lingua.English, lingua.Spanish).
		WithMinimumRelativeDistance(0.9).
		Build()

	pass := business.Q4FilterReviews(&business.Review{
		AppID:       "1",
		AppName:     "test",
		ReviewText:  "This is a review that is in english",
		ReviewScore: 1,
		ReviewVotes: 100,
	}, func(s string) bool {
		lang, exists := detector.DetectLanguageOf(s)
		return exists && lang == lingua.English
	}, true)

	if pass != true {
		t.Fatal("The review didn't pass the filter")
	}
}

func TestQ4ReviewFilteringSpanish(t *testing.T) {
	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(lingua.English, lingua.Spanish).
		WithMinimumRelativeDistance(0.9).
		Build()

	pass := business.Q4FilterReviews(&business.Review{
		AppID:       "1",
		AppName:     "test",
		ReviewText:  "Esto es una reseña en español",
		ReviewScore: 1,
		ReviewVotes: 100,
	}, func(s string) bool {
		lang, exists := detector.DetectLanguageOf(s)
		return exists && lang == lingua.English
	}, true)

	if pass != false {
		t.Fatal("The review passed the filter when it shouldn't the filter")
	}
}
