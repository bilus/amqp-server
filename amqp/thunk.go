package amqp

// Thunk is a trampoline thunk used to implement tail-recursive functions without TCO.
type Thunk interface {
	error
	Invoke() Thunk
	Done() bool
	Err() error
}

type thunk struct {
	f func() Thunk
	error
}

// Invoke runs function wrapped by thunk.
func (t thunk) Invoke() Thunk {
	return t.f()
}

// Done returns true to terminate computation.
func (t thunk) Done() bool {
	return t.f == nil
}

// Err returns error, if any.
func (t thunk) Err() error {
	return t.error
}

// Delay turns a function into a thunk.
func Delay(f func() Thunk) Thunk {
	return thunk{f, nil}
}

// Fail signals irrevocable failure, terminating computation with an error.
func Fail(err error) Thunk {
	return thunk{nil, err}
}

// Finish terminates computation without error.
func Finish() Thunk {
	return thunk{nil, nil}
}

var _ Thunk = thunk{}
