package bus_test

import (
	"sync"
	"testing"
	"time"

	"github.com/bilus/amqp-server/bus"
	"github.com/bilus/amqp-server/test/require"
)

type Outbox struct {
	deliveries []Message
	mtx        sync.Mutex
}

func (o *Outbox) Deliver(m Message) {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	o.deliveries = append(o.deliveries, m)
}

func (o *Outbox) Reset() {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	o.deliveries = nil
}

func (o *Outbox) Messages() []Message {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	return append([]Message(nil), o.deliveries...)
}

type Message string

func TestBus_Broadcast(t *testing.T) {
	require := require.New(t)

	actualDeliveries := Outbox{}
	bus := bus.New[Message](1)
	sub := bus.Subscribe("topic", func(m Message) {
		actualDeliveries.Deliver(m)
	})

	expectedDeliveries := []Message{"m1", "m2", "m3"}
	for _, m := range expectedDeliveries {
		numConsumers, numDelivered, numDropped := bus.Broadcast("topic", m)
		require.Equal(1, numConsumers)
		require.Equal(1, numDelivered+numDropped, "no delivery guarantees")
	}
	require.Eventually(bus.Idle, time.Millisecond*100, time.Millisecond)
	require.Subset(expectedDeliveries, actualDeliveries.Messages())

	err := bus.Unsubscribe(sub)
	require.NoError(err)

	actualDeliveries.Reset()
	numConsumers, numDelivered, numDropped := bus.Broadcast("topic", Message("foo"))
	require.Equal(0, numConsumers)
	require.Equal(0, numDelivered)
	require.Equal(0, numDropped)
	require.Empty(actualDeliveries.Messages())

	err = bus.Unsubscribe(sub)
	require.Error(err)
}

func TestBus_Deliver(t *testing.T) {
	require := require.New(t)

	actualDeliveries := Outbox{}
	bus := bus.New[Message](1)
	sub := bus.Subscribe("topic", func(m Message) {
		actualDeliveries.Deliver(m)
	})

	expectedDeliveries := []Message{"m1", "m2", "m3"}
	for _, m := range expectedDeliveries {
		numDelivered := bus.Deliver("topic", m)
		require.Equal(1, numDelivered)
	}
	require.Eventually(bus.Idle, time.Millisecond*100, time.Millisecond)
	require.ElementsMatch(expectedDeliveries, actualDeliveries.Messages())

	err := bus.Unsubscribe(sub)
	require.NoError(err)

	actualDeliveries.Reset()
	numDelivered := bus.Deliver("topic", Message("foo"))
	require.Equal(0, numDelivered)
	require.Empty(actualDeliveries.Messages())

	err = bus.Unsubscribe(sub)
	require.Error(err)
}
