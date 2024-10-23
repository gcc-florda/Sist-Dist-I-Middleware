package business_test

import (
	"middleware/common"
	"middleware/worker/business"
	"testing"

	"github.com/pemistahl/lingua-go"
	"github.com/spf13/viper"
)

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	common.Config = v
	return v, nil
}
func TestQ4ReviewFilteringEnglish(t *testing.T) {
	InitConfig()
	common.Config.SetDefault("query.three.positive", false)
	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(lingua.English, lingua.Spanish).
		WithMinimumRelativeDistance(0.9).
		Build()

	pass := business.Q4FilterReviewsBuilder(func(s string) bool {
		lang, exists := detector.DetectLanguageOf(s)
		return exists && lang == lingua.English
	})(&business.Review{
		AppID:       "1",
		AppName:     "test",
		ReviewText:  "This is a review that is in english",
		ReviewScore: 1,
		ReviewVotes: 100,
	})

	if pass != true {
		t.Fatal("The review didn't pass the filter")
	}
}

func TestQ4ReviewFilteringSpanish(t *testing.T) {
	InitConfig()
	common.Config.SetDefault("query.3.positive", false)
	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(lingua.English, lingua.Spanish).
		WithMinimumRelativeDistance(0.9).
		Build()

	pass := business.Q4FilterReviewsBuilder(func(s string) bool {
		lang, exists := detector.DetectLanguageOf(s)
		return exists && lang == lingua.English
	})(&business.Review{
		AppID:       "1",
		AppName:     "test",
		ReviewText:  "Esto es una reseña en español",
		ReviewScore: 1,
		ReviewVotes: 100,
	})

	if pass != false {
		t.Fatal("The review passed the filter when it shouldn't the filter")
	}
}
