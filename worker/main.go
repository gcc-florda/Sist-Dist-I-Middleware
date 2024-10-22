package main

import (
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/controller"
	"sync"

	"github.com/op/go-logging"
)

type ControllerFactory func(cfg *ControllerConfig, arcCfg *rabbitmq.ArchitectureConfig, arc *rabbitmq.Architecture) *controller.Controller

var log = logging.MustGetLogger("log")

var controllerFactories = map[string]ControllerFactory{
	"MFGQ1": CreateMFGQ1,
	"MFGQ2": CreateMFGQ2,
	"MFGQ3": CreateMFGQ3,
	"MFGQ4": CreateMFGQ4,
	"MFGQ5": CreateMFGQ5,
	"MFRQ3": CreateMFRQ3,
	"MFRQ4": CreateMFRQ4,
	"MFRQ5": CreateMFRQ5,
	"Q1S2":  CreateQ1S2,
	"Q1S3":  CreateQ1S3,
	"Q2S2":  CreateQ2S2,
	"Q2S3":  CreateQ2S3,
	"Q3S2":  CreateQ3S2,
	"Q3S3":  CreateQ3S3,
	"Q4S2":  CreateQ4S2,
	"Q4S3":  CreateQ4S3,
	"Q5S2":  CreateQ5S2,
	"Q5S3":  CreateQ5S3,
}

func main() {
	var arcCfg = rabbitmq.LoadConfig("./architecture.yaml")
	var arc = rabbitmq.CreateArchitecture(arcCfg)
	var _, err = common.InitConfig("./common.yaml")
	var controllersConfig = LoadConfig("./controllers.yaml")
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	for _, controllerConfig := range controllersConfig.Controllers {
		c, ok := controllerFactories[controllerConfig.Type]
		if !ok {
			log.Fatalf("Can't find controller for %s", controllerConfig.Type)
		}
		wg.Add(1)

		go func(cfg ControllerConfig) {
			defer wg.Done()
			c(&cfg, arcCfg, arc).Start()
		}(controllerConfig)

	}

	wg.Wait()
}
