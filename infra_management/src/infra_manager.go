package src

import (
	"fmt"
	"middleware/common"
	"os"
	"strconv"
)

type InfraManager struct {
	Id             int
	WorkersManager *WorkerStatusManager
}

func NewInfraManager() (*InfraManager, error) {
	id, err := strconv.Atoi(os.Getenv("MANAGER_ID"))

	if err != nil {
		log.Criticalf("Error parsing manager id: %s", err)
		return nil, err
	}

	return &InfraManager{
		Id:             id,
		WorkersManager: NewWorkerStatusManager(),
	}, nil
}

func (m *InfraManager) Start(workerPort string) error {
	log.Debug("Loading architecture")

	err := m.LoadArchitecture()

	if err != nil {
		log.Criticalf("Error loading architecture: %s", err)
		return err
	}

	m.ListenForWorkers(workerPort)

	return nil
}

func (m *InfraManager) ListenForWorkers(workerPort string) {
	for _, worker := range m.WorkersManager.Workers {
		log.Debugf("Establishing connection with worker %s", worker.Name)
		worker.Listener = workerPort
		worker.EstablishConnection()

		log.Debugf("Handling worker %s", worker.Name)
		go worker.Handle()
		log.Debugf("Finish handling worker %s", worker.Name)
	}
}

func (m *InfraManager) LoadArchitecture() error {
	arcCfg := common.LoadArchitectureConfig("./architecture.yaml")

	// MAP FILTER

	for i := range arcCfg.MapFilter.QueryOneGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq1_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryTwoGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq2_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryThreeGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq3_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryFourGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq4_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryFiveGames.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfgq5_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryThreeReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq3_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryFourReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq4_%d", i+1))
	}

	for i := range arcCfg.MapFilter.QueryFiveReviews.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_mfrq5_%d", i+1))
	}

	// S2

	for i := range arcCfg.QueryOne.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q1s2_%d", i+1))
	}

	for i := range arcCfg.QueryTwo.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q2s2_%d", i+1))
	}

	for i := range arcCfg.QueryThree.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q3s2_%d", i+1))
	}

	for i := range arcCfg.QueryFour.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q4s2_%d", i+1))
	}

	for i := range arcCfg.QueryFive.StageTwo.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q5s2_%d", i+1))
	}

	// S3

	for i := range arcCfg.QueryOne.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q1s3_%d", i+1))
	}

	for i := range arcCfg.QueryTwo.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q2s3_%d", i+1))
	}

	for i := range arcCfg.QueryThree.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q3s3_%d", i+1))
	}

	for i := range arcCfg.QueryFour.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q4s3_%d", i+1))
	}

	for i := range arcCfg.QueryFive.StageThree.PartitionAmount {
		m.WorkersManager.AddWorker(fmt.Sprintf("node_q5s3_%d", i+1))
	}

	return nil
}
