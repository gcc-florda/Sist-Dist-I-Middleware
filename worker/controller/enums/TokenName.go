package enums

type TokenName uint32

const (
	CLIENT_GAMES_EOF TokenName = iota //For these two, we need just one that comes in the Queue
	CLIENT_REVIEWS_EOF
	MF_GAMES //For these two, we need an amount of EOFS equal to the partition
	MF_REVIEWS
	SINGLE_STREAM_EOF //For these, we need an amount of EOFS equal to the partition
	TokenName_end
)

func IsValidTokenName(n uint32) bool {
	return n < uint32(TokenName_end)
}
