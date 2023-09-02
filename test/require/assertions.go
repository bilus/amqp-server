package require

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bilus/amqp-server/amqp"

	rabbitmq "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type Assertions struct {
	*require.Assertions
}

type readWriteCloser struct {
	io.Reader
	io.Writer
	io.Closer
}

func New(t *testing.T) *Assertions {
	return &Assertions{
		require.New(t),
	}
}

func (require *Assertions) OpenConnection(ctx context.Context, heartbeat time.Duration) (*rabbitmq.Connection, *amqp.Connection) {
	rc, ws := io.Pipe()
	rs, wc := io.Pipe()

	serverConn := amqp.NewConnection(rs, ws, ws)
	go func() error {
		serverConn.Do(ctx)
		return nil
	}()

	uri, err := rabbitmq.ParseURI("amqp://foo")
	require.NoError(err)
	config := rabbitmq.Config{
		Vhost:     "/",
		Heartbeat: 1 * time.Second, // TODO: Test hearbeat negotiation.
		SASL:      []rabbitmq.Authentication{uri.PlainAuth()},
	}
	clientConn, err := rabbitmq.Open(readWriteCloser{rc, wc, wc}, config)
	require.NoError(err)

	return clientConn, serverConn
}
