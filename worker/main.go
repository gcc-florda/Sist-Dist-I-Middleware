package worker

import (
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/business"
	"middleware/worker/controller"
)

func main() {
	// Building a controller example
	rabbit := rabbitmq.NewRabbit()

	queue := rabbit.NewQueue("MAP_FILTER_GAMES_Q1")

	exc := rabbit.NewExchange("Q1_STAGE_2", common.ExchangeDirect)

	c := controller.NewController(
		[]*rabbitmq.Queue{
			queue,
		},
		[]*rabbitmq.Exchange{
			exc,
		},
		&controller.NodeProtocol{
			PartitionAmount: 3,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			return &business.MapFilterGames{
				Filter: nil,
				Mapper: business.Q1Map,
			}, controller.NewEOFChecker("Q1_STAGE_2", 1), nil
		},
	)

	c.Start()
}
