package amqp

import (
	"sync/atomic"
	"time"

	"github.com/paulbellamy/ratecounter"
)

type Stats struct {
	numOpenConnections int64

	outgoingRate *ratecounter.RateCounter

	dropRate           *ratecounter.RateCounter
	numDroppedMessages uint64
}

func NewStats() *Stats {
	return &Stats{
		outgoingRate: ratecounter.NewRateCounter(1 * time.Second),
		dropRate:     ratecounter.NewRateCounter(1 * time.Second),
	}
}

func (stats *Stats) OnMessagesBroadcasted(n int64) {
	stats.outgoingRate.Incr(n)
}

func (stats *Stats) OnMessagesDropped(n int64) {
	stats.dropRate.Incr(n)
	atomic.AddUint64(&stats.numDroppedMessages, uint64(n))
}

func (stats *Stats) OutgoingRate() int64 {
	return stats.outgoingRate.Rate()
}

func (stats *Stats) DropRate() int64 {
	return stats.dropRate.Rate()
}

func (stats *Stats) NumDroppedMessages() uint64 {
	return atomic.LoadUint64(&stats.numDroppedMessages)
}

func (stats *Stats) OnConnectionOpen() {
	atomic.AddInt64(&stats.numOpenConnections, 1)
}

func (stats *Stats) OnConnectionClosed() {
	atomic.AddInt64(&stats.numOpenConnections, -1)
}

func (stats *Stats) NumOpenConnections() int64 {
	return atomic.LoadInt64(&stats.numOpenConnections)
}
