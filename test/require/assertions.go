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

type AMQPServer struct {
	require *Assertions
	users   []amqp.User
}

func New(t *testing.T) *Assertions {
	return &Assertions{
		require.New(t),
	}
}

func (require *Assertions) OpenConnection(ctx context.Context, heartbeat time.Duration) (*rabbitmq.Connection, *amqp.Connection) {
	server := require.AMQPServer(ctx)
	clientConn, serverConn, stop, err := server.Connect(ctx, "amqp://localhost/", heartbeat)
	go func() {
		<-ctx.Done()
		stop()
	}()
	require.NoError(err)
	return clientConn, serverConn
}

func (require *Assertions) OpenConnection2(ctx context.Context, heartbeat time.Duration) (*rabbitmq.Connection, *amqp.Connection, func()) {
	server := require.AMQPServer(ctx)
	clientConn, serverConn, stop, err := server.Connect(ctx, "amqp://localhost/", heartbeat)
	require.NoError(err)
	return clientConn, serverConn, stop
}

func (require *Assertions) AMQPServer(ctx context.Context, credentials ...string) AMQPServer {
	if len(credentials) == 0 {
		return AMQPServer{require, nil}
	}
	require.Len(credentials, 2)
	username := credentials[0]
	password := credentials[1]
	return AMQPServer{
		require, []amqp.User{
			{
				Username: username,
				Password: password,
			},
		},
	}
}

func (server AMQPServer) Connect(ctx context.Context, uri string, heartbeat time.Duration) (*rabbitmq.Connection, *amqp.Connection, func(), error) {
	require := server.require

	rc, ws := io.Pipe()
	rs, wc := io.Pipe()

	var serverConn *amqp.Connection
	if len(server.users) == 0 {
		serverConn = amqp.NewConnection(rs, ws, ws)
	} else {
		serverConn = amqp.NewConnection(rs, ws, ws, amqp.WithAuth(amqp.NewSimpleAuth(server.users...)))
	}

	go serverConn.Do(ctx)

	amqpURI := require.rabbitmqURI(uri)

	config := rabbitmq.Config{
		Vhost:     "/",
		Heartbeat: heartbeat,
		SASL:      []rabbitmq.Authentication{amqpURI.PlainAuth()},
	}
	clientConn, err := rabbitmq.Open(readWriteCloser{rc, wc, wc}, config)
	if err != nil {
		require.True(clientConn.IsClosed())
		require.True(serverConn.IsDead())
		return nil, nil, nil, err
	}

	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())

	// Automatically clean up.
	stop := func() {
		ws.Close()
		wc.Close()
		rs.Close()
		rc.Close()
		clientConn.Close()
		serverConn.Close()
	}
	return clientConn, serverConn, stop, err
}

func (require *Assertions) rabbitmqURI(uri string) rabbitmq.URI {
	u, err := rabbitmq.ParseURI(uri)
	require.NoError(err)
	return u
}

func (require *Assertions) WithChannel(ctx context.Context, f func(ch *rabbitmq.Channel, clientConn *rabbitmq.Connection, serverConn *amqp.Connection)) {
	// Slow heartbeat so it doesn't interfere with tests.
	heartbeat := time.Second * 10
	clientConn, serverConn := require.OpenConnection(ctx, heartbeat)
	ch, err := clientConn.Channel()
	require.NoError(err)
	f(ch, clientConn, serverConn)
}
