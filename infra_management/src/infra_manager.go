package src

import (
	"fmt"
	"middleware/common"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

type InfraManager struct {
	WorkersManager *WorkerStatusManager
	ReplicaManager *ReplicaManager
	term           chan os.Signal
}

func NewInfraManager(ringIp string, ringPort string, ringReplicasAmount int) (*InfraManager, error) {
	id, err := strconv.Atoi(os.Getenv("MANAGER_ID"))

	if err != nil {
		log.Criticalf("Error parsing manager id: %s", err)
		return nil, err
	}

	m := &InfraManager{
		ReplicaManager: NewReplicaManager(id, ringReplicasAmount, ringIp, ringPort),
		WorkersManager: NewWorkerStatusManager(),
		term:           make(chan os.Signal, 1),
	}

	signal.Notify(m.term, syscall.SIGTERM)

	return m, nil
}

func (m *InfraManager) Start(workerPort string) error {
	go m.HandleShutdown()

	go m.ReplicaManager.Start()

	log.Debug("Loading architecture")

	err := m.LoadArchitecture()

	if err != nil {
		log.Criticalf("Error loading architecture: %s", err)
		return err
	}

	for msg := range m.ReplicaManager.CoordNews {
		if msg {
			m.ListenForWorkers(workerPort)
		} else {
			log.Infof("I am no longer the coordinator")
			m.StopHandleWorkers()
		}
	}

	return nil
}

func (m *InfraManager) ListenForWorkers(workerPort string) {
	for _, worker := range m.WorkersManager.Workers {
		select {
		case msg := <-m.ReplicaManager.CoordNews:
			if !msg {
				log.Infof("I am no longer the coordinator")
				m.StopHandleWorkers()
				return
			}
		default:
			worker.port = workerPort
			go worker.Watch()
		}
	}
}

func (m *InfraManager) StopHandleWorkers() {
	for _, worker := range m.WorkersManager.Workers {
		worker.CoordNews <- false
	}
}

func (m *InfraManager) LoadArchitecture() error {
	arcCfg := common.LoadArchitectureConfig("./architecture.yaml")

	m.WorkersManager.AddWorker("server")

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

func (m *InfraManager) HandleShutdown() {
	<-m.term
	log.Criticalf("Received SIGTERM")

	m.WorkersManager.HandleShutdown()

	m.ReplicaManager.HandleShutdown()
}
