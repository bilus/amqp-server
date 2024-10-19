package bus

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

// MessageBus implements publish/subscribe messaging paradigm
type MessageBus[M any] interface {
	// Deliver publishes m to the given topic subscribers, blocking only when
	// the buffer of one of the subscribers is full.
	Deliver(topic string, m M) (numDelivered int)
	// Broadcast attempts to deliver m to the given topic subscribers, dropping
	// the message for subscribers whose buffers are full.
	Broadcast(topic string, m M) (numConsumers int, numDelivered int, numDropped int)
	// Close unsubscribe all handlers from given topic
	Close(topic string)
	// Subscribe subscribes to the given topic
	Subscribe(topic string, fn func(M)) *Subscription[M]
	// Unsubscribe unsubscribe handler from the given topic
	Unsubscribe(sub *Subscription[M]) error
}

type handlersMap[M any] map[string][]*Subscription[M]

type Subscription[M any] struct {
	topic    string
	callback func(M)
	queue    chan M
}

type messageBus[M any] struct {
	handlerQueueSize int
	mtx              sync.RWMutex
	handlers         handlersMap[M]
}

// New creates new MessageBus
// handlerQueueSize sets buffered channel length per subscriber
func New[M any](handlerQueueSize int) *messageBus[M] {
	if handlerQueueSize == 0 {
		panic("handlerQueueSize has to be greater then 0")
	}

	return &messageBus[M]{
		handlerQueueSize: handlerQueueSize,
		handlers:         make(handlersMap[M]),
	}
}

func (b *messageBus[M]) Broadcast(topic string, m M) (numConsumers int, numDelivered int, numDropped int) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			select {
			case h.queue <- m:
				numDelivered++
			default:
				log.Println("Dropping msg to", topic)
				numDropped++
			}
		}
		numConsumers = len(hs)
		return numConsumers, numDelivered, numDropped
	}
	return 0, 0, 0
}

func (b *messageBus[M]) Deliver(topic string, m M) (numDelivered int) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if hs, ok := b.handlers[topic]; ok {
		for _, h := range hs {
			h.queue <- m
		}
		return len(hs)
	}
	return 0
}

func (b *messageBus[M]) Subscribe(topic string, fn func(M)) *Subscription[M] {
	h := &Subscription[M]{
		topic:    topic,
		callback: fn,
		queue:    make(chan M, b.handlerQueueSize),
	}

	go func() {
		for m := range h.queue {
			h.callback(m)
		}
	}()

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.handlers[topic] = append(b.handlers[topic], h)

	return h
}

func (b *messageBus[M]) Unsubscribe(sub *Subscription[M]) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	found := false
	topic := sub.topic
	handlers, ok := b.handlers[topic]
	if !ok {
		return errors.New("no such topic")
	}
	tmp := handlers[:0]
	for _, h := range handlers {
		if h == sub {
			found = true
			close(h.queue)
		} else {
			tmp = append(tmp, h)
		}
	}
	b.handlers[topic] = tmp
	if !found {
		return fmt.Errorf("no such subscription")
	}
	return nil
}

func (b *messageBus[M]) Close(topic string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, ok := b.handlers[topic]; ok {
		for _, h := range b.handlers[topic] {
			close(h.queue)
		}

		delete(b.handlers, topic)

		return
	}
}

func (b *messageBus[M]) Idle() bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for _, hs := range b.handlers {
		for _, h := range hs {
			if len(h.queue) > 0 {
				return false
			}
		}
	}
	return true
}
