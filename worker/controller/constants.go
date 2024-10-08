package controller

import "middleware/worker/controller/enums"

var TokensNeeded = map[string][]enums.TokenName{
	"MAP_FILTER_GAMES":   {enums.CLIENT_GAMES_EOF},
	"MAP_FILTER_REVIEWS": {enums.CLIENT_REVIEWS_EOF},
	"Q1_STAGE_2":         {enums.MF_GAMES},
	"Q1_STAGE_3":         {enums.SINGLE_STREAM_EOF},
	"Q2_STAGE_2":         {enums.MF_GAMES},
	"Q2_STAGE_3":         {enums.SINGLE_STREAM_EOF},
	"Q3_STAGE_2":         {enums.MF_GAMES, enums.MF_REVIEWS},
	"Q3_STAGE_3":         {enums.SINGLE_STREAM_EOF},
	"Q4_STAGE_2":         {enums.MF_GAMES, enums.MF_REVIEWS},
	"Q4_STAGE_3":         {enums.SINGLE_STREAM_EOF},
	"Q5_STAGE_2":         {enums.MF_GAMES, enums.MF_REVIEWS},
	"Q5_STAGE_3":         {enums.SINGLE_STREAM_EOF},
}

var TokenToSend = map[string]enums.TokenName{
	"MAP_FILTER_GAMES":   enums.MF_GAMES,
	"MAP_FILTER_REVIEWS": enums.MF_REVIEWS,
	"Q1_STAGE_2":         enums.SINGLE_STREAM_EOF,
	"Q1_STAGE_3":         enums.SINGLE_STREAM_EOF,
	"Q2_STAGE_2":         enums.SINGLE_STREAM_EOF,
	"Q2_STAGE_3":         enums.SINGLE_STREAM_EOF,
	"Q3_STAGE_2":         enums.SINGLE_STREAM_EOF,
	"Q3_STAGE_3":         enums.SINGLE_STREAM_EOF,
	"Q4_STAGE_2":         enums.SINGLE_STREAM_EOF,
	"Q4_STAGE_3":         enums.SINGLE_STREAM_EOF,
	"Q5_STAGE_2":         enums.SINGLE_STREAM_EOF,
	"Q5_STAGE_3":         enums.SINGLE_STREAM_EOF,
}
