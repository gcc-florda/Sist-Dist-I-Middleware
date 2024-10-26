package schema

import "middleware/common"

type Partitionable interface {
	common.Serializable
	PartitionKey() string
}
