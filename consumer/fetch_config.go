package consumer

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type Provided struct{}

func (Provided) q() {}

type validated struct{}

func (validated) q() {}

// Marker interface to statically ensure we only accept validated FetchConfigs.
type configQuality interface {
	q()
}

// FetchConfig configures the Consumer's fetching behavior when retrieving records from Kafka.
type FetchConfig[t configQuality] struct {
	// MinBytes controls the minimum amount of data the server should return for a fetch request.
	MinBytes          int32
	MaxBytes          int32
	PartitionMaxBytes int32
	MaxWait           time.Duration
	IsolationLevel    kafka.IsolationLevel
}

// Validate returns a correct FetchConfig, and indicates whether any corrections were applied.
//
// Defaults that will be applied are:
//
// - MinBytes: 1
// - MaxBytes: 50 MiB
// - PartitionMaxBytes: 1 MiB
// - MaxWait: 10 seconds
// - IsolationLevel: Read Uncommitted
func (fc FetchConfig[t]) Validate() (FetchConfig[validated], bool) {
	corrected := false

	validConfig := FetchConfig[validated]{
		MinBytes:          fc.MinBytes,
		MaxBytes:          fc.MaxBytes,
		PartitionMaxBytes: fc.PartitionMaxBytes,
		MaxWait:           fc.MaxWait,
		IsolationLevel:    fc.IsolationLevel,
	}

	if validConfig.MinBytes == 0 {
		corrected = true
		validConfig.MinBytes = 1
	}

	if validConfig.MaxBytes == 0 {
		corrected = true
		validConfig.MaxBytes = 50 * 1024 * 1024
	}

	if validConfig.PartitionMaxBytes == 0 {
		corrected = true
		validConfig.PartitionMaxBytes = 1024 * 1024
	}

	if validConfig.MaxWait == 0 {
		corrected = true
		validConfig.MaxWait = 10 * time.Second
	}

	return validConfig, corrected
}
