package main

import (
	"middleware/common/utils"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	v, err := utils.InitConfig("./client/config.yaml")
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := utils.InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	utils.PrintConfig(v)

	// clientConfig := common.ClientConfig{
	// 	ServerAddress:   v.GetString("server.address"),
	// 	BatchMaxAmount:  v.GetInt("batch.maxAmount"),
	// 	BatchSleep:      v.GetDuration("batch.sleep"),
	// 	GamesFilePath:   "/app/datasets/games.csv",
	// 	ReviewsFilePath: "/app/datasets/reviews.csv",
	// }

	// client := common.NewClient(clientConfig)
	// client.StartClient()
}
