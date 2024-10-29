package main

import (
	"fmt"
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/business"
	"middleware/worker/controller"
)

func CreateQ1S2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q1S2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryOne.StageTwo.GetQueueSingle(cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryOne.StageThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ1(common.Config.GetString("savepath"), jobId.String(), cfg.ReadFromPartition, "stage_two")
			if err != nil {
				return nil, nil, err
			}
			return h, controller.NewEOFChecker("Q1_STAGE_2", uint(arcCfg.MapFilter.QueryOneGames.PartitionAmount)), nil
		},
	)
}

func CreateQ2S2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q2S2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryTwo.StageTwo.GetQueueSingle(cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryTwo.StageThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ2(
				common.Config.GetString("savepath"),
				"stage_two",
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("query.two.top"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h, controller.NewEOFChecker("Q2_STAGE_2", uint(arcCfg.MapFilter.QueryTwoGames.PartitionAmount)), nil
		},
	)
}

func CreateQ3S2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q3S2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryThree.StageTwo.GetQueueSingle(cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryThree.StageThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewJoin(
				common.Config.GetString("savepath"),
				"query_three",
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("joinBuffer"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h,
				controller.NewEOFChecker(
					"Q3_STAGE_2",
					uint(arcCfg.MapFilter.QueryThreeGames.PartitionAmount),
					uint(arcCfg.MapFilter.QueryThreeReviews.PartitionAmount),
				),
				nil
		},
	)
}

func CreateQ4S2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q4S2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryFour.StageTwo.GetQueueSingle(cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFour.StageThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewJoin(
				common.Config.GetString("savepath"),
				"query_four",
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("joinBuffer"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h,
				controller.NewEOFChecker(
					"Q4_STAGE_2",
					uint(arcCfg.MapFilter.QueryFourGames.PartitionAmount),
					uint(arcCfg.MapFilter.QueryFourReviews.PartitionAmount),
				), nil
		},
	)
}

func CreateQ5S2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q5S2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryFive.StageTwo.GetQueueSingle(cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFive.StageThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewJoin(
				common.Config.GetString("savepath"),
				"query_five",
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("joinBuffer"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h,
				controller.NewEOFChecker(
					"Q5_STAGE_2",
					uint(arcCfg.MapFilter.QueryFiveGames.PartitionAmount),
					uint(arcCfg.MapFilter.QueryFiveReviews.PartitionAmount),
				),
				nil
		},
	)
}
