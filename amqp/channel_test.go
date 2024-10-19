package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/bilus/amqp-server/test/require"
)

// TODO(bilus): Simplify using test suite.

func TestOpenClose(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, serverConn := require.OpenConnection(ctx, time.Millisecond*1)

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

	clientConn, _ := require.OpenConnection(ctx, time.Millisecond*1)

	ch1, _ := clientConn.Channel()
	err := ch1.Close()
	require.NoError(err)
	require.True(ch1.IsClosed())

	ch2, err := clientConn.Channel()
	require.NoError(err)
	require.False(ch2.IsClosed())
}

func TestQueueDeclare(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, _ := require.OpenConnection(ctx, time.Millisecond*1)

	ch1, _ := clientConn.Channel()

	q, err := ch1.QueueDeclare("", false, false, true, false, nil)
	require.NoError(err)
	require.NotEmpty(q.Name)
}

func TestQueueDeclare_OnlyOnePerConnection(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, _ := require.OpenConnection(ctx, time.Millisecond*1)
	ch1, _ := clientConn.Channel()
	_, _ = ch1.QueueDeclare("", false, false, true, false, nil)

	_, err := ch1.QueueDeclare("", false, false, true, false, nil)
	require.Error(err)
	require.True(ch1.IsClosed())
	require.False(clientConn.IsClosed())
}

func TestQueueDeclare_ValidConfigurations(t *testing.T) {
	// log.SetLevel(log.DebugLevel)

	req := require.New(t)

	testCases := []struct {
		desc                                   string
		queueName                              string
		durable, autoDelete, exclusive, noWait bool
		expectedSuccess                        bool
	}{
		{
			"exclusive queue",
			"",
			false, false, true, false,
			true,
		},
		{
			"autoDelete has no effect",
			"",
			false, true, true, false,
			true,
		},
		{
			"noWait has no effect",
			"",
			false, false, true, true,
			true,
		},
		{
			"queue name must be server-generated",
			"XX",
			false, false, true, false,
			false,
		},
		{
			"durable queues are not supported",
			"",
			true, false, true, false,
			false,
		},
		{
			"queue must be exclusive",
			"",
			false, false, false, false,
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			clientConn, serverConn := req.OpenConnection(ctx, time.Millisecond*1)
			ch1, _ := clientConn.Channel()

			_, err := ch1.QueueDeclare(tc.queueName, tc.durable, tc.autoDelete, tc.exclusive, tc.noWait, nil)
			if tc.expectedSuccess {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ch1.IsClosed())
				require.False(clientConn.IsClosed())
				require.False(serverConn.IsDead())
			}
		})
	}
}

func TestQueueDelete(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, _ := require.OpenConnection(ctx, time.Millisecond*1)

	ch1, _ := clientConn.Channel()

	q1, _ := ch1.QueueDeclare("", false, false, true, false, nil)
	_, err := ch1.QueueDelete("BADBAD", false, false, false)
	require.Error(err, "Cannot delete a non-existent queue")
	require.True(ch1.IsClosed())
	require.False(clientConn.IsClosed())

	ch2, err := clientConn.Channel()
	require.NoError(err)

	numPurged, err := ch2.QueueDelete(q1.Name, false, false, false)
	require.NoError(err)
	require.Zero(numPurged)
}

func TestQueueDeclare_UniqueQueueNames(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	clientConn, _ := require.OpenConnection(ctx, time.Millisecond*1)

	ch1, _ := clientConn.Channel()

	q1, _ := ch1.QueueDeclare("", false, false, true, false, nil)
	numPurged, err := ch1.QueueDelete(q1.Name, false, false, false)
	require.NoError(err)
	require.Zero(numPurged)

	q2, err := ch1.QueueDeclare("", false, false, true, false, nil)
	require.NoError(err)

	require.NotEqual(q1.Name, q2.Name)
}

// TODO:
// Cannot bind to non-existent queue
// QueueDelete
// Client closes channel
// Consume
// Second call to Consume
