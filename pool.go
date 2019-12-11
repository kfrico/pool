package pool

import "errors"

var (
	// ErrClosed連接池已經關閉Error
	ErrClosed = errors.New("pool is closed")
)

// Pool 基本方法
type Pool interface {
	Get() (interface{}, error)

	Put(interface{}) error

	Close(interface{}) error

	Release()

	Len() int
}
