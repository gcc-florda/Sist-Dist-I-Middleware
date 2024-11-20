package main

import (
	"fmt"
	"middleware/common"
	"net"
	"sync"
	"time"
)

type InfraManager struct {
	Id             int
	Address        string
	Listener       net.Listener
	WorkersManager *WorkerStatusManager
}

func NewInfraManager(id int, ip string, port int) *InfraManager {
	return &InfraManager{
		Id:             id,
		Address:        fmt.Sprintf("%s:%d", ip, port),
		WorkersManager: NewWorkerStatusManager(),
	}
}

func (m *InfraManager) Start(configFilePath string) error {
	var err error
	m.Listener, err = net.Listen("tcp", m.Address)
	common.FailOnError(err, "Failed to start manager")
	defer m.Listener.Close()

	log.Infof("Manager listening on %s", m.Address)

	var wg sync.WaitGroup

	wg.Add(2)

	go m.ListenForWorkers()

	go m.WatchDeadWorkers()

	wg.Wait()

	return nil
}

func (m *InfraManager) ListenForWorkers() {
	for {
		conn, err := m.Listener.Accept()
		if err != nil {
			log.Errorf("Action: Accept connection | Result: Error | Error: %s", err)
			break
		}

		go m.HandleWorker(conn)
	}
}

func (m *InfraManager) HandleWorker(conn net.Conn) error {
	defer conn.Close()

	message, err := common.Receive(conn)

	if err != nil {
		log.Errorf("Action: Receive Message from Worker | Result: Error | Error: %s", err)
		return err // TODO: Review --> how should we handle the manager when a worker establish the connection but can't send it's first message (it's name)
	}

	workerName := common.ManagementMessage{Content: message}

	if !workerName.IsName() {
		return fmt.Errorf("expecting Worker Name")
	}

	workerStatus := m.WorkersManager.GetWorkerStatusByName(workerName.Content)

	if workerStatus == nil {
		return fmt.Errorf("worker not found")
	}

	workerStatus.UpdateWorkerStatus(true, conn)

	m.Watch(workerStatus)

	return nil
}

func (m *InfraManager) Watch(worker *WorkerStatus) {
	for {
		if worker.Send("HCK") != nil {
			worker.UpdateWorkerStatus(false, nil)
			break
		}

		message, err := worker.Receive()

		if err != nil {
			worker.UpdateWorkerStatus(false, nil)
			break
		} else {
			messageAlive := common.ManagementMessage{Content: message}

			if !messageAlive.IsAlive() {
				worker.UpdateWorkerStatus(false, nil)
				break
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (m *InfraManager) WatchDeadWorkers() {
	for {
		deadWorker := m.WorkersManager.GetDeadWorker()

		if deadWorker != nil {
			deadWorker.Revive()
		}
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
