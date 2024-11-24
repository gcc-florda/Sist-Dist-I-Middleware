package main

import (
	"fmt"
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/business"
	"middleware/worker/controller"

	"github.com/pemistahl/lingua-go"
)

func CreateMFGQ1(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFGQ1_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Games.GetQueue("MFG_Q1", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryOne.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryOne.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {

			mf, err := business.NewMapFilterGames(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q1G",
				cfg.ReadFromPartition,
				business.Q1Map,
				nil,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
		},
	)
}

func CreateMFGQ2(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFGQ2_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Games.GetQueue("MFG_Q2", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryTwo.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryTwo.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterGames(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q2G",
				cfg.ReadFromPartition,
				business.Q2Map,
				business.Q2Filter,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
		},
	)
}

func CreateMFGQ3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFGQ3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Games.GetQueue("MFG_Q3", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryThree.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryThree.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterGames(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q3G",
				cfg.ReadFromPartition,
				business.Q3MapGames,
				business.Q3FilterGames,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
		},
	)
}

func CreateMFGQ4(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFGQ4_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Games.GetQueue("MFG_Q4", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFour.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryFour.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterGames(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q4G",
				cfg.ReadFromPartition,
				business.Q4MapGames,
				business.Q4FilterGames,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
		},
	)
}

func CreateMFGQ5(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFGQ5_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Games.GetQueue("MFG_Q5", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFive.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryFive.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterGames(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q5G",
				cfg.ReadFromPartition,
				business.Q5MapGames,
				business.Q5FilterGames,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_GAMES", 1), nil
		},
	)
}

func CreateMFRQ3(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFRQ3_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Reviews.GetQueue("MFR_Q3", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryThree.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryThree.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterReviews(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q3R",
				cfg.ReadFromPartition,
				business.Q3MapReviews,
				business.Q3FilterReviews,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_REVIEWS", 1), nil
		},
	)
}

func CreateMFRQ4(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	detector := lingua.NewLanguageDetectorBuilder().
		FromLanguages(lingua.English, lingua.Spanish).
		WithMinimumRelativeDistance(0.9).
		Build()

	f := func(s string) bool {
		lang, exists := detector.DetectLanguageOf(s)
		return exists && lang == lingua.English
	}

	return controller.NewController(
		fmt.Sprintf("MFRQ4_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Reviews.GetQueue("MFR_Q4", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFour.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryFour.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterReviews(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q4R",
				cfg.ReadFromPartition,
				business.Q4MapReviews,
				business.Q4FilterReviewsBuilder(f),
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_REVIEWS", 1), nil
		},
	)
}

func CreateMFRQ5(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller {
	return controller.NewController(
		fmt.Sprintf("MFRQ5_%d", cfg.ReadFromPartition),
		[]*rabbitmq.Queue{
			arc.MapFilter.Reviews.GetQueue("MFR_Q5", cfg.ReadFromPartition),
		},
		[]*rabbitmq.Exchange{
			arc.QueryFive.StageTwo.GetExchange(),
		},
		&controller.NodeProtocol{
			PartitionAmount: uint(arcCfg.QueryFive.StageTwo.PartitionAmount),
		},
		func(jobId common.JobID) (controller.Handler, controller.EOFValidator, error) {
			mf, err := business.NewMapFilterReviews(
				common.Config.GetString("savepath"),
				jobId.String(),
				"Q5R",
				cfg.ReadFromPartition,
				business.Q5MapReviews,
				business.Q5FilterReviews,
			)

			if err != nil {
				return nil, nil, err
			}

			return mf, controller.NewEOFChecker("MAP_FILTER_REVIEWS", 1), nil
		},
	)
}
