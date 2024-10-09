package business

import (
	"encoding/csv"
	"middleware/common"
	"reflect"
	"strconv"
	"strings"
)

type Game struct {
	AppID                   string
	Name                    string
	ReleaseDate             string
	EstimatedOwners         string
	PeakCCU                 string
	RequiredAge             string
	Price                   string
	Discount                string
	DLCCount                string
	AboutTheGame            string
	SupportedLanguages      string
	FullAudioLanguages      string
	Reviews                 string
	HeaderImage             string
	Website                 string
	SupportURL              string
	SupportEmail            string
	Windows                 bool
	Mac                     bool
	Linux                   bool
	MetacriticScore         string
	MetacriticURL           string
	UserScore               string
	Positive                string
	Negative                string
	ScoreRank               string
	Achievements            string
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
	Windows uint32
	Linux   uint32
	Mac     uint32
}

func (s *SOCounter) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteUint32(s.Windows).WriteUint32(s.Linux).WriteUint32(s.Mac).ToBytes()
}

func (s *SOCounter) PartitionKey() string {
	return common.GenerateRandomString(10)
}

func SOCounterDeserialize(d *common.Deserializer) (*SOCounter, error) {
	windows, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	linux, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	mac, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	return &SOCounter{
		Windows: windows,
		Linux:   linux,
		Mac:     mac,
	}, nil
}

type PlayedTime struct {
	AveragePlaytimeForever float64
	Name                   string
}

func (p *PlayedTime) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteFloat64(p.AveragePlaytimeForever).WriteString(p.Name).ToBytes()
}

func (p *PlayedTime) PartitionKey() string {
	return p.Name
}

func PlayedTimeDeserialize(d *common.Deserializer) (*PlayedTime, error) {
	pt, err := d.ReadFloat64()
	if err != nil {
		return nil, err
	}
	n, err := d.ReadString()
	if err != nil {
		return nil, err
	}
	return &PlayedTime{
		AveragePlaytimeForever: pt,
		Name:                   n,
	}, nil
}

type GameName struct {
	AppID string
	Name  string
}

func (g *GameName) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteString(g.AppID).WriteString(g.Name).ToBytes()
}

func (g *GameName) PartitionKey() string {
	return g.AppID
}

func GameNameDeserialize(d *common.Deserializer) (*GameName, error) {
	id, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	n, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	return &GameName{
		AppID: id,
		Name:  n,
	}, nil
}

type ValidReview struct {
	AppID string
}

func (v *ValidReview) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteString(v.AppID).ToBytes()
}

func (v *ValidReview) PartitionKey() string {
	return v.AppID
}

func ValidReviewDeserialize(d *common.Deserializer) (*ValidReview, error) {
	i, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	return &ValidReview{
		AppID: i,
	}, nil
}

type ReviewCounter struct {
	AppID string
	Count uint32
}

func (c *ReviewCounter) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteString(c.AppID).WriteUint32(c.Count).ToBytes()
}

func (c *ReviewCounter) PartitionKey() string {
	return c.AppID
}

func ReviewCounterDeserialize(d *common.Deserializer) (*ReviewCounter, error) {
	id, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	c, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	return &ReviewCounter{
		AppID: id,
		Count: c,
	}, nil
}

type NamedReviewCounter struct {
	Name  string
	Count uint32
}

func (c *NamedReviewCounter) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteString(c.Name).WriteUint32(c.Count).ToBytes()
}

func (c *NamedReviewCounter) PartitionKey() string {
	return c.Name
}

func NamedReviewCounterDeserialize(d *common.Deserializer) (*NamedReviewCounter, error) {
	n, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	c, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	return &NamedReviewCounter{
		Name:  n,
		Count: c,
	}, nil
}

func StrParse[T any](s string) (*T, error) {
	var z T
	reader := csv.NewReader(strings.NewReader(s))

	row, err := reader.Read()

	if err != nil {
		return nil, err
	}

	err = mapCSVToStruct(row, &z)

	if err != nil {
		return nil, err
	}

	return &z, nil
}

func mapCSVToStruct(row []string, result interface{}) error {
	v := reflect.ValueOf(result).Elem()

	for i := range row {
		// Just assume that the field are in order
		field := v.Field(i)

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
	}
	return nil
}

func parseSlice(s string) []string {
	return strings.Split(strings.Trim(s, `"`), ",")
}

func MarshalMessage(c any) ([]byte, error) {
	s := common.NewSerializer()
	switch v := c.(type) {
	case SOCounter:
		return s.WriteUint8(common.Type_SOCounter).WriteBytes(v.Serialize()).ToBytes(), nil
	case PlayedTime:
		return s.WriteUint8(common.Type_PlayedTime).WriteBytes(v.Serialize()).ToBytes(), nil
	case GameName:
		return s.WriteUint8(common.Type_GameName).WriteBytes(v.Serialize()).ToBytes(), nil
	case ValidReview:
		return s.WriteUint8(common.Type_ValidReview).WriteBytes(v.Serialize()).ToBytes(), nil
	case ReviewCounter:
		return s.WriteUint8(common.Type_ReviewCounter).WriteBytes(v.Serialize()).ToBytes(), nil
	case NamedReviewCounter:
		return s.WriteUint8(common.Type_NamedReviewCounter).WriteBytes(v.Serialize()).ToBytes(), nil
	}
	return nil, &UnknownTypeError{}
}

func UnmarshalMessage(messageBytes []byte) (any, error) {
	d := common.NewDeserializer(messageBytes)
	return UnmarshalMessageDeserializer(&d)
}

func UnmarshalMessageDeserializer(d *common.Deserializer) (any, error) {
	t, err := d.ReadUint8()
	if err != nil {
		return nil, err
	}

	switch t {
	case common.Type_Game:
		s, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		return StrParse[Game](s)
	case common.Type_Review:
		s, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		return StrParse[Review](s)
	case common.Type_SOCounter:
		return SOCounterDeserialize(d)
	case common.Type_PlayedTime:
		return PlayedTimeDeserialize(d)
	case common.Type_GameName:
		return GameNameDeserialize(d)
	case common.Type_ValidReview:
		return ValidReviewDeserialize(d)
	case common.Type_ReviewCounter:
		return ReviewCounterDeserialize(d)
	case common.Type_NamedReviewCounter:
		return NamedReviewCounterDeserialize(d)
	}
	return nil, &UnknownTypeError{}
}

type UnknownTypeError struct {
}

func (e *UnknownTypeError) Error() string {
	return "The provided type is not something that is known"
}
