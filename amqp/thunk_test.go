package amqp_test

import (
	"github.com/bilus/amqp-server/amqp"
	"testing"
)

func BenchmarkThunkStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		thunk := foo(0)
		for !thunk.Done() {
			thunk = thunk.Invoke()
		}
	}
}

func foo(i int) amqp.Thunk {
	return amqp.Delay(func() amqp.Thunk { return bar(i + 1) })
}

func bar(i int) amqp.Thunk {
	if i > 1 {
		return amqp.Finish()
	}

	return amqp.Delay(func() amqp.Thunk { return foo(i + 1) })
}

// thunk is a trampoline thunk.
type thunk func() (thunk, error)

func BenchmarkThunkFunc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		thunk, _ := foo2(0)
		for thunk != nil {
			thunk, _ = thunk()
		}
	}
}

func foo2(i int) (thunk, error) {
	return func() (thunk, error) { return bar2(i + 1) }, nil
}

func bar2(i int) (thunk, error) {
	if i > 1 {
		return nil, nil
	}

	return func() (thunk, error) { return foo2(i + 1) }, nil
}
