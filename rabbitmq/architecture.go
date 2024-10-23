package rabbitmq

import (
	"fmt"
	"middleware/common"
)

type Architecture struct {
	MapFilter  *MapFilterArchitecture
	QueryOne   *TwoStageArchitecture
	QueryTwo   *TwoStageArchitecture
	QueryThree *TwoStageArchitecture
	QueryFour  *TwoStageArchitecture
	QueryFive  *TwoStageArchitecture
	Results    *Results
	rabbit     *Rabbit
}

func CreateArchitecture(cfg *ArchitectureConfig) *Architecture {
	rabbit := NewRabbit()

	return &Architecture{
		MapFilter:  CreateMapFilterArchitecture(rabbit, cfg),
		QueryOne:   CreateTwoStageArchitecture(rabbit, &cfg.QueryOne, "Q1"),
		QueryTwo:   CreateTwoStageArchitecture(rabbit, &cfg.QueryTwo, "Q2"),
		QueryThree: CreateTwoStageArchitecture(rabbit, &cfg.QueryThree, "Q3"),
		QueryFour:  CreateTwoStageArchitecture(rabbit, &cfg.QueryFour, "Q4"),
		QueryFive:  CreateTwoStageArchitecture(rabbit, &cfg.QueryFive, "Q5"),
		Results:    CreateResults(rabbit),
		rabbit:     rabbit,
	}
}

func (a *Architecture) Close() {
	a.rabbit.Close()
}

type PartitionedQueues struct {
	queues []*Queue
	count  int
}

func CreatePartitionedQueues(rabbit *Rabbit, bindTo *Exchange, prefix string, count int) *PartitionedQueues {
	queues := make([]*Queue, 0, count)
	for i := 1; i <= count; i++ {
		q := rabbit.NewQueue(fmt.Sprintf("%s_%d", prefix, i))
		q.Bind(bindTo, fmt.Sprintf("%d", i))
		queues = append(queues, q)
	}
	return &PartitionedQueues{queues: queues, count: count}
}

func (q *PartitionedQueues) GetQueue(partitionKey int) *Queue {
	if partitionKey > q.count {
		log.Fatalf("Can't get a queue with partition key greater than the queues created (get: %d, created: %d)", partitionKey, q.count)
	}
	return q.queues[partitionKey-1]
}

type PartitionedExchange struct {
	exchange *Exchange
	channels map[string]*PartitionedQueues
}

func (e *PartitionedExchange) GetExchange() *Exchange {
	return e.exchange
}

func (e *PartitionedExchange) GetQueue(channel string, partitionKey int) *Queue {
	pqs, ok := e.channels[channel]
	if !ok {
		log.Fatalf("Couldn't get the channel %s for the exchange %s", channel, e.exchange.Name)
	}
	return pqs.GetQueue(partitionKey)
}

func (e *PartitionedExchange) GetQueueSingle(partitionKey int) *Queue {
	if len(e.channels) != 1 {
		log.Fatalf("Multiple channels for the exchange %s", e.exchange.Name)
	}
	for k := range e.channels {
		return e.channels[k].GetQueue(partitionKey)
	}
	// Dead code
	return nil
}

type MapFilterArchitecture struct {
	Games   *PartitionedExchange
	Reviews *PartitionedExchange
}

func createGamePartitionedExchange(rabbit *Rabbit, cfg *ArchitectureConfig) *PartitionedExchange {
	gex := rabbit.NewExchange("MAP_FILTER_GAMES", common.ExchangeDirect)

	channels := map[string]*PartitionedQueues{
		"MFG_Q1": CreatePartitionedQueues(rabbit, gex, "MFG_Q1", cfg.MapFilter.QueryOneGames.PartitionAmount),
		"MFG_Q2": CreatePartitionedQueues(rabbit, gex, "MFG_Q2", cfg.MapFilter.QueryTwoGames.PartitionAmount),
		"MFG_Q3": CreatePartitionedQueues(rabbit, gex, "MFG_Q3", cfg.MapFilter.QueryThreeGames.PartitionAmount),
		"MFG_Q4": CreatePartitionedQueues(rabbit, gex, "MFG_Q4", cfg.MapFilter.QueryFourGames.PartitionAmount),
		"MFG_Q5": CreatePartitionedQueues(rabbit, gex, "MFG_Q5", cfg.MapFilter.QueryFiveGames.PartitionAmount),
	}
	return &PartitionedExchange{
		exchange: gex,
		channels: channels,
	}
}

func createReviewPartitionedExchange(rabbit *Rabbit, cfg *ArchitectureConfig) *PartitionedExchange {
	gex := rabbit.NewExchange("MAP_FILTER_GAMES", common.ExchangeDirect)

	channels := map[string]*PartitionedQueues{
		"MFR_Q3": CreatePartitionedQueues(rabbit, gex, "MFR_Q3", cfg.MapFilter.QueryThreeReviews.PartitionAmount),
		"MFR_Q4": CreatePartitionedQueues(rabbit, gex, "MFR_Q4", cfg.MapFilter.QueryFourReviews.PartitionAmount),
		"MFR_Q5": CreatePartitionedQueues(rabbit, gex, "MFR_Q5", cfg.MapFilter.QueryFiveReviews.PartitionAmount),
	}
	return &PartitionedExchange{
		exchange: gex,
		channels: channels,
	}
}

func CreateMapFilterArchitecture(rabbit *Rabbit, cfg *ArchitectureConfig) *MapFilterArchitecture {
	return &MapFilterArchitecture{
		Games:   createGamePartitionedExchange(rabbit, cfg),
		Reviews: createReviewPartitionedExchange(rabbit, cfg),
	}
}

type TwoStageArchitecture struct {
	StageTwo   *PartitionedExchange
	StageThree *PartitionedExchange
}

func createStage(rabbit *Rabbit, partitionAmount int, name string) *PartitionedExchange {
	ex := rabbit.NewExchange(name, common.ExchangeDirect)

	return &PartitionedExchange{
		exchange: ex,
		channels: map[string]*PartitionedQueues{
			name: CreatePartitionedQueues(rabbit, ex, name, partitionAmount),
		},
	}
}

func CreateTwoStageArchitecture(rabbit *Rabbit, cfg *TwoStageConfig, name string) *TwoStageArchitecture {
	return &TwoStageArchitecture{
		StageTwo:   createStage(rabbit, cfg.StageTwo.PartitionAmount, fmt.Sprintf("%s_S2", name)),
		StageThree: createStage(rabbit, 1, fmt.Sprintf("%s_S3", name)),
	}
}

type Results struct {
	QueryOne   *PartitionedExchange
	QueryTwo   *PartitionedExchange
	QueryThree *PartitionedExchange
	QueryFour  *PartitionedExchange
	QueryFive  *PartitionedExchange
}

func createResult(rabbit *Rabbit, name string) *PartitionedExchange {
	ex := rabbit.NewExchange(name, common.ExchangeDirect)
	q := rabbit.NewQueue(name)
	q.Bind(ex, "")
	return &PartitionedExchange{
		exchange: ex,
		channels: map[string]*PartitionedQueues{
			name: CreatePartitionedQueues(rabbit, ex, name, 1),
		},
	}
}

func CreateResults(rabbit *Rabbit) *Results {
	return &Results{
		QueryOne:   createResult(rabbit, "Q1RESULT"),
		QueryTwo:   createResult(rabbit, "Q2RESULT"),
		QueryThree: createResult(rabbit, "Q3RESULT"),
		QueryFour:  createResult(rabbit, "Q4RESULT"),
		QueryFive:  createResult(rabbit, "Q5RESULT"),
	}
}
