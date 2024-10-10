package common

const (
	ACK = "ACK\n"
	END = "END\n"
)

const (
	TypeGame   = 1
	TypeReview = 2
)

const (
	ExchangeNameRawData = "raw_data"
)

const (
	ExchangeDirect = "direct"
	ExchangeFanout = "fanout"
	ExchangeTopic  = "topic"
)

const (
	RoutingGames    = "games"
	RoutingReviews  = "reviews"
	RoutingProtocol = "protocol"
)

type MessageType = uint8

const (
	Type_Game MessageType = iota
	Type_Review
	Type_SOCounter
	Type_PlayedTime
	Type_GameName
	Type_ValidReview
	Type_ReviewCounter
	Type_NamedReviewCounter
)
