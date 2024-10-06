package business

import (
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Game struct {
	AppID                   string
	Name                    string
	ReleaseDate             time.Time
	EstimatedOwners         int
	PeakCCU                 int
	RequiredAge             int
	Price                   float64
	DiscountDLCCount        int
	AboutTheGame            string
	SupportedLanguages      []string
	FullAudioLanguages      []string
	Reviews                 string
	HeaderImage             string
	Website                 string
	SupportURL              string
	SupportEmail            string
	Windows                 bool
	Mac                     bool
	Linux                   bool
	MetacriticScore         float64
	MetacriticURL           string
	UserScore               float64
	Positive                int
	Negative                int
	ScoreRank               int
	Achievements            int
	Recommendations         string
	Notes                   string
	AveragePlaytimeForever  float64
	AveragePlaytimeTwoWeeks float64
	MedianPlaytimeForever   float64
	MedianPlaytimeTwoWeeks  float64
	Developers              []string
	Publishers              []string
	Categories              []string
	Genres                  []string
	Tags                    []string
	Screenshots             []string
	Movies                  []string
}

type Review struct {
	AppID       string
	AppName     string
	ReviewText  string
	ReviewScore int
	ReviewVotes int
}

type SOCounter struct {
	Windows bool
	Linux   bool
	Mac     bool
}

func MapCSVToStruct(row []string, result interface{}) error {
	v := reflect.ValueOf(result).Elem()

	for i := range row {
		// Just assume that the field are in order
		field := v.Field(i)

		if !field.IsValid() {
			continue
		}

		if err := setFieldValue(field, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func setFieldValue(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int:
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		field.SetInt(int64(intValue))
	case reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		field.SetFloat(floatValue)
	case reflect.Bool:
		field.SetBool(value == "true")
	case reflect.Slice:
		field.Set(reflect.ValueOf(parseSlice(value)))
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			date, err := time.Parse("2006-01-02", value)
			if err != nil {
				return err
			}
			field.Set(reflect.ValueOf(date))
		}
	}
	return nil
}

func parseSlice(s string) []string {
	return strings.Split(strings.Trim(s, `"`), ";")
}
