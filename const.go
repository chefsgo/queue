package queue

import "errors"

const (
	NAME = "queue"
)

var (
	errInvalidConnection = errors.New("Invalid queue connection.")
)
