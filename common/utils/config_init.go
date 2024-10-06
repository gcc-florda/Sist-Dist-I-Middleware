package utils

import (
	"os"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig(path string) (*viper.Viper, error) {
	v := viper.New()

	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		// Let's not laod from environment. While nice it adds quite a bit of code that is not needed
		// for our scope
		log.Fatalf("Configuration couldn't be read from config file. %s", err)
	}

	return v, nil
}

func PrintConfig(cfg *viper.Viper) {
	settings := cfg.AllSettings()

	// Iterate through the map and print each key-value pair
	for key, value := range settings {
		log.Infof("Loaded configuration %s: %v\n", key, value)
	}
}

func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	logging.SetBackend(backendLeveled)
	return nil
}
