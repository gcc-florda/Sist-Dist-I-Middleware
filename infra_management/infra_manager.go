package main

import (
	"fmt"
	"middleware/common"
)

type InfraManager struct {
	Id             int
	WorkersManager *WorkerStatusManager
}

func NewInfraManager(id int) *InfraManager {
	return &InfraManager{
		Id:             id,
		WorkersManager: NewWorkerStatusManager(),
	}
}

func (m *InfraManager) Start(workerPort string) error {

	m.ListenForWorkers(workerPort)

	return nil
}

func (m *InfraManager) ListenForWorkers(workerPort string) {

	for _, worker := range m.WorkersManager.Workers {
		worker.EstablishConnection(workerPort)

		go worker.Handle()
	}
}

func (m *InfraManager) LoadArchitecture() error {
	arcCfg := common.LoadArchitectureConfig("./architecture.yaml")

	// MAP FILTER

	for i := range arcCfg.MapFilter.QueryOneGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq1_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryTwoGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq2_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryThreeGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq3_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryFourGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq4_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryFiveGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq5_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryThreeReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq3_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryFourReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq4_%d", i))
	}

	for i := range arcCfg.MapFilter.QueryFiveReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq5_%d", i))
	}

	// S2

	for i := range arcCfg.QueryOne.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q1s2_%d", i))
	}

	for i := range arcCfg.QueryTwo.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q2s2_%d", i))
	}

	for i := range arcCfg.QueryThree.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q3s2_%d", i))
	}

	for i := range arcCfg.QueryFour.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q4s2_%d", i))
	}

	for i := range arcCfg.QueryFive.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q5s2_%d", i))
	}

	// S3

	for i := range arcCfg.QueryOne.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q1s3_%d", i))
	}

	for i := range arcCfg.QueryTwo.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q2s3_%d", i))
	}

	for i := range arcCfg.QueryThree.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q3s3_%d", i))
	}

	for i := range arcCfg.QueryFour.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q4s3_%d", i))
	}

	for i := range arcCfg.QueryFive.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q5s3_%d", i))
	}

	return nil
}
