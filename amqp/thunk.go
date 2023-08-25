package amqp

// Thunk is a trampoline thunk, for implementing tail-recursive functions
// without stack-exhaustion, otherwise impossible because Golang doesn't do TCO.
// Tail-recursive functions provide a straightforward way to implement the
// protocol using callbacks, instead of channels, a faster and less
// memory-intensive implementation.
//
// Semantics:
//
// 1. Continuation (delayed call to foo), no error:
//
//	return func() (Thunk, error) { return foo() }, nil
//
// 2. Failure; terminate computation with an error:
//
//	return nil, errors.New("Oops!")
//
// 3. Stop: terminate computation without an error:
//
//	return nil, nil
//
// 4. Continuation with an error (e.g. to log an error and retry):
//
//	return func() (Thunk, error) { return foo() }, errors.New("Oops!")
//
// Attempts to provide a better API by wrapping the function in a struct
// result in a 3 x worse performance than using straight (Thunk, error)
// return value.
//
// See [Connection.Start] for how thunks are executed.
type Thunk func() (Thunk, error)
