package main

import (
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/business"
	"middleware/worker/controller"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	common.InitLogger("DEBUG")
	_, err := common.InitConfig("/app/config.yaml")
	if err != nil {
		panic("Nos fuimos")
	}
	rabbit := rabbitmq.NewRabbit()

	exG := rabbit.NewExchange(common.ExchangeNameGames, common.ExchangeFanout)
	MFG_Q1 := rabbit.NewQueue("MFG_Q1")
	MFG_Q1.Bind(exG, "")
	log.Debug("Created map and filter exchange and binded MFG_Q1")

	exQ1_2 := rabbit.NewExchange("Q1_2", common.ExchangeDirect)
	Q1_2 := rabbit.NewQueue("Q1_2")
	Q1_2.Bind(exQ1_2, "1")
	log.Debug("Created Q1 stage 2 exchange and binded Q1_2 with '1' ")

	exQ1_3 := rabbit.NewExchange("Q1_3", common.ExchangeDirect)
	Q1_3 := rabbit.NewQueue("Q1_3")
	Q1_3.Bind(exQ1_3, "1")
	log.Debug("Created Q1 stage 3 exchange and binded Q1_3 with '1' ")

	exR_Q1 := rabbit.NewExchange("RESULT_Q1", common.ExchangeFanout)
	R_Q1 := rabbit.NewQueue("RESULT_Q1")
	R_Q1.Bind(exR_Q1, "")
	log.Debug("Created Q1 Result exchange and binded ResultQ1")

	workerController := common.Config.GetString("controller")

	if workerController == "MAP_FILTER_GAMES_Q1" {
		log.Debug("This is a MFG_Q1")
		c := controller.NewController(
			[]*rabbitmq.Queue{
				MFG_Q1,
			},
			[]*rabbitmq.Exchange{
				exQ1_2,
			},
			&controller.NodeProtocol{
				PartitionAmount: 1,
			},
			func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
				return &business.MapFilterGames{
					Filter: nil,
					Mapper: business.Q1Map,
				}, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
			},
		)

		go c.Start()
	} else if workerController == "Q1_2" {
		log.Debug("This is a Q1_2")
		c := controller.NewController(
			[]*rabbitmq.Queue{
				Q1_2,
			},
			[]*rabbitmq.Exchange{
				exQ1_3,
			},
			&controller.NodeProtocol{
				PartitionAmount: 1,
			},
			func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
				check := controller.NewEOFChecker("Q1_STAGE_2", 1)
				h, err := business.NewQ1("./storage", jobId.String(), "stage_2")
				if err != nil {
					return nil, check, err
				}
				return h, check, nil
			},
		)

		go c.Start()
	} else if workerController == "Q1_3" {
		log.Debug("This is a Q1_3")
		c := controller.NewController(
			[]*rabbitmq.Queue{
				Q1_3,
			},
			[]*rabbitmq.Exchange{
				exR_Q1,
			},
			&controller.NodeProtocol{
				PartitionAmount: 1,
			},
			func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
				check := controller.NewEOFChecker("Q1_STAGE_3", 1)
				h, err := business.NewQ1("./storage", jobId.String(), "stage_3")
				if err != nil {
					return nil, check, err
				}
				return h, check, nil
			},
		)

		go c.Start()
	}
}
