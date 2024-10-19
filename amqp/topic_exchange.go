package amqp

import (
	"log"
	"sync"
	"time"
	"unsafe"

	"github.com/bilus/amqp-server/bus"
)

type (
	RoutingKey = string
	Message    struct {
		Body       []byte
		RoutingKey RoutingKey
		Timestamp  time.Time
	}
	connAddr        = uint64
	connTrackingMap = map[connAddr][]*bus.Subscription[Message]
)

// subscriptionQueueSize controls the size of the buffer channel for each subscription.
// For a large number of subscriptions it'll affect RAM but increase tolerance for occassional peaks.
const subscriptionQueueSize = 1

// TopicExchange implements a simplistic AMQP topic exchange, without support
// for wildcard patterns.
type TopicExchange struct {
	Name string
	// RAM: 1 channel per connection = sizeof(Message) * subscriptionQueueSize * activeConnections
	bus bus.MessageBus[Message]

	mtx sync.Mutex
	// RAM: 1 entry * activeConnections
	connections connTrackingMap

	Stats *Stats
}

// NewTopicExchange creates a new topic exchange.
func NewTopicExchange(name string, stats *Stats) TopicExchange {
	return TopicExchange{
		Name:        name,
		bus:         bus.New[Message](subscriptionQueueSize),
		connections: make(connTrackingMap),
		Stats:       stats,
	}
}

func (exchange *TopicExchange) Publish(m Message) {
	// Messages that cannot be delivered because the subscription's buffer channel is full will be not delivered.
	// See subscriptionQueueSize for how to control that.
	_, numDelivered, numDropped := exchange.bus.Broadcast(m.RoutingKey, m)
	if numDelivered > 0 {
		exchange.Stats.OnMessagesBroadcasted(int64(numDelivered))
	}
	if numDropped > 0 {
		exchange.Stats.OnMessagesDropped(int64(numDropped))
	}
}

func (exchange *TopicExchange) QueueBind(routingKey RoutingKey, conn *Connection, channelID ChannelID) {
	// TODO(bilus): channel ID is ignored because only 1 AMQP chan is supported for now.
	sub := exchange.bus.Subscribe(routingKey, func(m Message) {
		if err := conn.DeliverMessage(m, exchange.Name, channelID); err != nil {
			if !conn.IsDead() {
				log.Printf("Error delivering message: %v", err)
			}
		}
	})
	connAddr := conAddr(conn)
	exchange.mtx.Lock()
	defer exchange.mtx.Unlock()
	exchange.connections[connAddr] = append(exchange.connections[connAddr], sub)
}

func (exchange *TopicExchange) RemoveConnection(conn *Connection) {
	// log.Println("[DEBUG] Removing connection from exchange")
	connAddr := conAddr(conn)
	exchange.mtx.Lock()
	defer exchange.mtx.Unlock()
	for _, sub := range exchange.connections[connAddr] {
		_ = exchange.bus.Unsubscribe(sub)
	}
	delete(exchange.connections, connAddr)
}

func conAddr(conn *Connection) connAddr {
	ptr := (*uint64)(unsafe.Pointer(conn))
	return *ptr
}
