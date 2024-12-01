package common

import (
	"bytes"
	"math/rand"
	"os/exec"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func GenerateRandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func ReviveContainer(name string, maxRetries int) error {
	for i := 1; i <= maxRetries; i++ {

		log.Debugf("Reviving container %s", name)

		// Kill the container first
		stopCmd := exec.Command("docker", "stop", name)
		var stopOut, stopErr bytes.Buffer
		stopCmd.Stdout = &stopOut
		stopCmd.Stderr = &stopErr

		if err := stopCmd.Run(); err != nil {
			log.Infof("DOCKER STOP | Error while stoping container %s: %v", name, err)

			if i == maxRetries {
				log.Errorf("DOCKER STOP | Max retries reached for container %s", name)
				return err
			}

			time.Sleep(10 * time.Second)

			continue
		} else {
			log.Infof("DOCKER STOP | Container %s stopped", name)
		}

		// Revive the container
		startCmd := exec.Command("docker", "start", name)
		var startOut, startErr bytes.Buffer
		startCmd.Stdout = &startOut
		startCmd.Stderr = &startErr

		if err := startCmd.Run(); err != nil {
			log.Infof("DOCKER START | Error while starting container %s: %v", name, err)

			if i == maxRetries {
				log.Errorf("DOCKER START | Max retries reached for container %s", name)
				return err
			}

			time.Sleep(10 * time.Second)

			continue
		} else {
			log.Infof("DOCKER START | Container %s started", name)
		}

		log.Debugf("Container revived: %s", name)

		return nil
	}

	return nil
}
