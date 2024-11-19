package main

import (
	"fmt"
	"middleware/common"
	"net"
	"strings"
	"sync"
	"time"
)

type InfraManager struct {
	Id       int
	Address  string
	Listener net.Listener
	Workers  []WorkerStatus
}

func NewInfraManager(id int, ip string, port int) *InfraManager {
	return &InfraManager{
		Id:      id,
		Address: fmt.Sprintf("%s:%d", ip, port),
		Workers: []WorkerStatus{},
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

	workerStatus := m.GetWorkerStatusByName(workerName.Content)

	if workerStatus == nil {
		return fmt.Errorf("worker not found")
	}

	workerStatus.Connection = conn
	workerStatus.Alive = true

	m.Watch(workerStatus)

	return nil
}

func (m *InfraManager) Watch(worker *WorkerStatus) {
	for {
		if common.Send("HCK\n", worker.Connection) != nil {
			worker.Alive = false
			worker.Connection.Close()
			break
		}

		message, err := common.Receive(worker.Connection)

		if err != nil {
			worker.Alive = false
			worker.Connection.Close()
			break
		} else {
			messageAlive := common.ManagementMessage{Content: message}

			if !messageAlive.IsAlive() {
				worker.Alive = false
				worker.Connection.Close()
				break
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (m *InfraManager) WatchDeadWorkers() {
	for {
		for i := range m.Workers {
			if !m.Workers[i].Alive {
				// TODO: I have to revive the worker
			}
		}
	}
}

func (m *InfraManager) GetWorkerStatusByName(name string) *WorkerStatus {
	for i := range m.Workers {
		archName := strings.Split(m.Workers[i].Name, "_")[0]
		if archName == name {
			return &m.Workers[i]
		}
	}

	return nil
}

func (m *InfraManager) LoadArchitecture() error {
	arcCfg := common.LoadArchitectureConfig("./architecture.yaml")

	// MAP FILTER

	for i := range arcCfg.MapFilter.QueryOneGames.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFGQ1_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryTwoGames.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFGQ2_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryThreeGames.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFGQ3_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryFourGames.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFGQ4_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryFiveGames.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFGQ5_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryThreeReviews.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFRQ3_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryFourReviews.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFRQ4_%d", i)))
	}

	for i := range arcCfg.MapFilter.QueryFiveReviews.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("MFRQ5_%d", i)))
	}

	// S2

	for i := range arcCfg.QueryOne.StageTwo.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q1S2_%d", i)))
	}

	for i := range arcCfg.QueryTwo.StageTwo.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q2S2_%d", i)))
	}

	for i := range arcCfg.QueryThree.StageTwo.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q3S2_%d", i)))
	}

	for i := range arcCfg.QueryFour.StageTwo.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q4S2_%d", i)))
	}

	for i := range arcCfg.QueryFive.StageTwo.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q5S2_%d", i)))
	}

	// S3

	for i := range arcCfg.QueryOne.StageThree.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q1S3_%d", i)))
	}

	for i := range arcCfg.QueryTwo.StageThree.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q2S3_%d", i)))
	}

	for i := range arcCfg.QueryThree.StageThree.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q3S3_%d", i)))
	}

	for i := range arcCfg.QueryFour.StageThree.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q4S3_%d", i)))
	}

	for i := range arcCfg.QueryFive.StageThree.PartitionAmount {
		m.Workers = append(m.Workers, *NewWorkerStatus(fmt.Sprintf("Q5S3_%d", i)))
	}

	return nil
}
