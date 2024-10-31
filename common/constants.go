package common

const (
	TypeGame   = 1
	TypeReview = 2
)

const (
	BROADCAST_ROUTING_KEY_GAMES   = "GAMES"
	BROADCAST_ROUTING_KEY_REVIEWS = "REVIEWS"
)

const (
	ExchangeDirect = "direct"
	ExchangeFanout = "fanout"
	ExchangeTopic  = "topic"
)

const (
	RoutingGames   = "games.*"
	RoutingReviews = "reviews.*"
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
