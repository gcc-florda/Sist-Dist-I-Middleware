package common

import "github.com/spf13/viper"

type PartitionConfig struct {
	PartitionAmount int `mapstructure:"partition_amount"`
}

type TwoStageConfig struct {
	StageTwo   PartitionConfig `mapstructure:"stage_two"`
	StageThree PartitionConfig `mapstructure:"stage_three"`
}

type ArchitectureConfig struct {
	MapFilter struct {
		QueryOneGames     PartitionConfig `mapstructure:"query_one_games"`
		QueryTwoGames     PartitionConfig `mapstructure:"query_two_games"`
		QueryThreeGames   PartitionConfig `mapstructure:"query_three_games"`
		QueryFourGames    PartitionConfig `mapstructure:"query_four_games"`
		QueryFiveGames    PartitionConfig `mapstructure:"query_five_games"`
		QueryThreeReviews PartitionConfig `mapstructure:"query_three_reviews"`
		QueryFourReviews  PartitionConfig `mapstructure:"query_four_reviews"`
		QueryFiveReviews  PartitionConfig `mapstructure:"query_five_reviews"`
	} `mapstructure:"map_filter"`

	QueryOne TwoStageConfig `mapstructure:"query_one"`

	QueryTwo TwoStageConfig `mapstructure:"query_two"`

	QueryThree TwoStageConfig `mapstructure:"query_three"`

	QueryFour TwoStageConfig `mapstructure:"query_four"`

	QueryFive TwoStageConfig `mapstructure:"query_five"`
}

func LoadArchitectureConfig(configFilePath string) *ArchitectureConfig {
	viper.SetConfigFile(configFilePath)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("fatal error reading config file for architecture load: %w", err)
	}

	var config ArchitectureConfig
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("unable to decode into struct: %w", err)
	}

	return &config
}
