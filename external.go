package queue

import (
	. "github.com/chefsgo/base"
)

// Publish
func Enqueue(name string, values ...Map) error {
	return module.Enqueue(name, values...)
}
