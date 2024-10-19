package amqp_test

import (
	"context"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	"github.com/bilus/amqp-server/amqp"
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
	require.Eventually(serverConn.IsDead, time.Millisecond*10, time.Millisecond)
	require.True(clientConn.IsClosed())
}

func TestConnection_HighChannelChurn(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	clientConn, _ := require.OpenConnection(ctx, heartbeat)

	for i := 0; i < 1000; i++ {
		ch, err := clientConn.Channel()
		require.NoError(err)
		require.False(ch.IsClosed())
		ch.Close()
		require.True(ch.IsClosed())
	}
	require.False(clientConn.IsClosed())
}

func TestConnection_Auth(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := require.AMQPServer(ctx, "good", "good")

	_, _, _, err := server.Connect(ctx, "amqp://bad:bad@localhost/", heartbeat)
	require.Error(err)

	clientConn, serverConn, stop, err := server.Connect(ctx, "amqp://good:good@localhost/", heartbeat)
	defer stop()
	require.NoError(err)
	require.False(clientConn.IsClosed())
	require.False(serverConn.IsDead())
}

func TestConnection_DeclareTopicExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require := require.New(t)

	require.WithChannel(ctx, func(ch *rabbitmq.Channel, _ *rabbitmq.Connection, _ *amqp.Connection) {
		err := ch.ExchangeDeclare("test", "topic", false, false, false, false, nil)
		require.NoError(err)
		err = ch.ExchangeDeclare("test", "topic", false, false, false, false, nil)
		require.NoError(err)
		err = ch.ExchangeDeclare("test2", "topic", false, false, false, false, nil)
		require.NoError(err)

		err = ch.ExchangeDeclare("test", "direct", false, false, false, false, nil)
		require.Error(err, "only topic exchanges are allowed")
		require.True(ch.IsClosed())
	})
}

func TestConnection_QueueDeclareTemporary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require := require.New(t)

	var q1 rabbitmq.Queue
	require.WithChannel(ctx, func(ch *rabbitmq.Channel, _ *rabbitmq.Connection, _ *amqp.Connection) {
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		require.NoError(err)
		require.NotEmpty(q.Name)
		q1 = q

		_, err = ch.QueueDeclare("", false, false, true, false, nil)
		require.Error(err, "LIMITATION: only one queue per connection allowed")
		require.True(ch.IsClosed())
	})

	// TODO: Once there's inter-connection state (e.g. exchange), this test
	// should be rewritten so that instead of creating new server, it
	// connects to the existing one.

	var q2 rabbitmq.Queue
	require.WithChannel(ctx, func(ch *rabbitmq.Channel, _ *rabbitmq.Connection, _ *amqp.Connection) {
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		require.NoError(err, "another connection should be able to declare a temporary queue")
		require.NotEmpty(q.Name)

		q2 = q
	})

	require.NotEqual(q1.Name, q2.Name, "temporary queues should have unique names")
}

func TestConnection_QueueDeclareNamed(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.WithChannel(ctx, func(ch *rabbitmq.Channel, conn *rabbitmq.Connection, _ *amqp.Connection) {
		q, err := ch.QueueDeclare("test", false, false, true, false, nil)
		require.NoError(err)
		require.Equal("test", q.Name)

		q2, err := ch.QueueDeclare("test2", false, false, false, false, nil)
		require.NoError(err)
		require.Equal("test2", q2.Name)
	})
}

// TestConnection_SubscribeTwoQueues
// TestConnection_SubscribeWildcard
// Two topic exchanges
