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
