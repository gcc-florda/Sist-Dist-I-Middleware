package main

import "github.com/spf13/viper"

type ControllerConfig struct {
	Type              string `mapstructure:"type"`
	ReadFromPartition int    `mapstructure:"readFromPartition"`
}

type ControllersConfig struct {
	Controllers []ControllerConfig `mapstructure:"controllers"`
}

func LoadConfig(configFilePath string) *ControllersConfig {
	viper.SetConfigFile(configFilePath)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("fatal error reading config file for node creation: %w", err)
	}

	var config ControllersConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("unable to decode into struct: %w", err)
	}

	return &config
}
