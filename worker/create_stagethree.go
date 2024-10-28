package main

import (
	"fmt"
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/business"
	"middleware/worker/controller"
)

func CreateQ1S3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q1S3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryOne.StageThree.GetQueueSingle(1),
		},
		[]*rabbitmq.Exchange{
			arc.Results.QueryOne.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ1(common.Config.GetString("savepath"), jobId.String(), cfg.ReadFromPartition, "stage_three")
			if err != nil {
				return nil, nil, err
			}
			return h, controller.NewEOFChecker("Q1_STAGE_3", uint(arcCfg.QueryOne.StageTwo.PartitionAmount)), nil
		},
	)
}

func CreateQ2S3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q2S3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryTwo.StageThree.GetQueueSingle(1),
		},
		[]*rabbitmq.Exchange{
			arc.Results.QueryTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ2(
				common.Config.GetString("savepath"),
				"stage_three",
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("query.two.top"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h, controller.NewEOFChecker("Q2_STAGE_3", uint(arcCfg.QueryTwo.StageTwo.PartitionAmount)), nil
		},
	)
}

func CreateQ3S3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q3S3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryThree.StageThree.GetQueueSingle(1),
		},
		[]*rabbitmq.Exchange{
			arc.Results.QueryThree.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ3(
				common.Config.GetString("savepath"),
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("query.three.top"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h, controller.NewEOFChecker("Q3_STAGE_3", uint(arcCfg.QueryThree.StageTwo.PartitionAmount)), nil
		},
	)
}

func CreateQ4S3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q4S3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryFour.StageThree.GetQueueSingle(1),
		},
		[]*rabbitmq.Exchange{
			arc.Results.QueryFour.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ4(
				common.Config.GetString("savepath"),
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("query.four.over"),
				common.Config.GetInt("joinBuffer"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h, controller.NewEOFChecker("Q4_STAGE_3", uint(arcCfg.QueryFour.StageTwo.PartitionAmount)), nil
		},
	)
}

func CreateQ5S3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("Q5S3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.QueryFive.StageThree.GetQueueSingle(1),
		},
		[]*rabbitmq.Exchange{
			arc.Results.QueryFive.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: 1,
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			h, err := business.NewQ5(
				common.Config.GetString("savepath"),
				jobId.String(),
				cfg.ReadFromPartition,
				common.Config.GetInt("query.five.percentile"),
				common.Config.GetInt("sortBuffer"),
			)

			if err != nil {
				return nil, nil, err
			}

			return h, controller.NewEOFChecker("Q4_STAGE_3", uint(arcCfg.QueryFive.StageTwo.PartitionAmount)), nil
		},
	)
}
