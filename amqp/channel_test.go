package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/bilus/amqp-server/test/require"
)

func TestOpenClose(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, serverConn := require.OpenConnection(ctx, time.Millisecond*1)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())

	ch, err := clientConn.Channel()
	require.NoError(err)
	require.False(ch.IsClosed())

	err = ch.Close()
	require.NoError(err)
	time.Sleep(10 * time.Millisecond)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())
}

func TestOpenClose_TwoChannels(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, serverConn := require.OpenConnection(ctx, time.Millisecond*1)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())

	ch1, err := clientConn.Channel()
	require.NoError(err)
	require.False(ch1.IsClosed())

	err = ch1.Close()
	require.NoError(err)

	ch2, err := clientConn.Channel()
	require.NoError(err)
	require.False(ch2.IsClosed())
}
