package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/bilus/amqp-server/test/require"
)

func TestConnectDisconnect(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, serverConn := require.OpenConnection(ctx, time.Second*10)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())

	serverConn.Close()
	require.Eventually(serverConn.IsDead, time.Millisecond, time.Microsecond)
	require.True(clientConn.IsClosed())
}
