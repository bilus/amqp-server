package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/bilus/amqp-server/test/require"
)

// Slow heartbeat so it doesn't interfere with tests.
var heartbeat = time.Second * 10

func TestConnection_ConnectDisconnect(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, serverConn := require.OpenConnection(ctx, heartbeat)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())

	serverConn.Close()
	require.Eventually(serverConn.IsDead, time.Millisecond, time.Microsecond)
	require.True(clientConn.IsClosed())
}

func TestConnection_Auth(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := require.AMQPServer(ctx, "good", "good")

	_, _, err := server.Connect(ctx, "amqp://bad:bad@localhost/", heartbeat)
	require.Error(err)

	clientConn, serverConn, err := server.Connect(ctx, "amqp://good:good@localhost/", heartbeat)
	require.NoError(err)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())
}
