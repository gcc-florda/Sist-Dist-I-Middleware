package main

import (
	"middleware/client/src"
	"middleware/common"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	v, err := common.InitConfig("./config.yaml")
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := common.InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	common.PrintConfig(v)

	clientConfig := src.ClientConfig{
		ServerAddress:   v.GetString("server.address"),
		BatchMaxAmount:  v.GetInt("batch.maxAmount"),
		BatchSleep:      v.GetDuration("batch.sleep"),
		GamesFilePath:   "/app/datasets/games_sample.csv",
		ReviewsFilePath: "/app/datasets/reviews_sample.csv",
	}

	client := src.NewClient(clientConfig)
	client.StartClient()
}
