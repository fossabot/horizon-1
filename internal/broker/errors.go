package broker

import "errors"

var (
	// ErrUnknownMember indicates the member is not part of the group
	ErrUnknownMember = errors.New("unknown member")

	// ErrIllegalGeneration indicates the generation ID is invalid
	ErrIllegalGeneration = errors.New("illegal generation")

	// ErrInconsistentProtocol indicates protocol type mismatch
	ErrInconsistentProtocol = errors.New("inconsistent group protocol")

	// ErrRebalanceInProgress indicates a rebalance is in progress
	ErrRebalanceInProgress = errors.New("rebalance in progress")

	// ErrGroupNotFound indicates the group does not exist
	ErrGroupNotFound = errors.New("group not found")
)
