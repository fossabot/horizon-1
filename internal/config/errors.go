package config

import "errors"

var (
	ErrInvalidPort        = errors.New("invalid port number: must be between 1 and 65535")
	ErrInvalidSegmentSize = errors.New("invalid segment size: must be at least 1 MB")
	ErrInvalidPartitions  = errors.New("invalid number of partitions: must be at least 1")
)
