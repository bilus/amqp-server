package amqp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"github.com/valinurovam/garagemq/amqp"
)

// Connection implements the state machine for an AMQP connection.
type Connection struct {
	rawConn           *RawConnection
	heartbeatInterval uint16
	maxChannels       uint16 // Future use, currently 1 chan max.
	maxFrameSize      uint32

	dead          int32
	declaredQueue string // Only one queue supported. Empty if not declared.

	authStrategy AuthStrategy
}

type consumer struct {
	tag       string
	channelID ChannelID
}

const (
	maxChannels              = 1
	maxFrameSize             = 65536
	defaultHeartbeatInterval = 10

	// controlChannelID MUST be zero for all heartbeat frames, and for method,
	// header and body frames that refer to the Connection class. A peer that
	// receives a non-zero channel number for one of these frames MUST signal a
	// connection exception with reply code 503 (command invalid).
	controlChannelID = 0
)

type option func(*Connection)

func WithAuth(auth AuthStrategy) option {
	return func(conn *Connection) {
		conn.authStrategy = auth
	}
}

func NewConnection(input io.Reader, output io.Writer, closer io.Closer, options ...option) *Connection {
	conn := &Connection{
		rawConn:           NewRawConnection(input, output, closer),
		heartbeatInterval: defaultHeartbeatInterval,
		maxChannels:       maxChannels,
		maxFrameSize:      maxFrameSize,
		authStrategy:      NoAuth{},
	}
	for _, opt := range options {
		opt(conn)
	}
	return conn
}

func (conn *Connection) Do(ctx context.Context) {
	defer conn.die()

	err := conn.rawConn.ReadAmqpHeader()
	if err != nil {
		conn.Close()
		return
	}

	capabilities := amqp.Table{}
	capabilities["publisher_confirms"] = false
	capabilities["exchange_exchange_bindings"] = false
	capabilities["basic.nack"] = true
	capabilities["consumer_cancel_notify"] = false
	capabilities["connection.blocked"] = false
	capabilities["consumer_priorities"] = false
	capabilities["authentication_failure_close"] = false
	capabilities["per_consumer_qos"] = false

	serverProps := amqp.Table{}
	serverProps["product"] = "github.com/bilus/amqp-server"
	serverProps["version"] = "0.1"
	serverProps["copyright"] = "Marcin Bilski"
	serverProps["platform"] = runtime.GOARCH
	serverProps["capabilities"] = capabilities
	host, err := os.Hostname()
	if err != nil {
		serverProps["host"] = "UnknownHostError"
	} else {
		serverProps["host"] = host
	}

	method := amqp.ConnectionStart{VersionMajor: 0, VersionMinor: 9, ServerProperties: &serverProps, Mechanisms: []byte("PLAIN"), Locales: []byte("en_US")}
	conn.SendMethod(&method, controlChannelID)

	thunk, err := conn.ReadFrame(ctx, conn.handleStarting)
	for {
		if err != nil {
			amqpError := amqpError{}
			if errors.As(err, &amqpError) {
				// Protocol error.
				thunk, err = conn.handleError(ctx, amqpError.ChannelID, amqpError.amqpErr)
				if err != nil {
					log.Printf("Error on chanel %d: %v", amqpError.ChannelID, err)
				}
			} else if err == ErrCloseAfter {
				conn.flushAndClose()
				return
			} else if err == ErrClientClosed {
				log.Println("Error: client unexpectedly closed connection")
				return
			} else {
				log.Printf("Error: %v", err)
			}
		}
		if thunk == nil {
			conn.Close()
			return
		}
		thunk, err = thunk()
	}
}

func (conn *Connection) SendMethod(method amqp.Method, channelID ChannelID) error {
	return conn.rawConn.SendMethod(method, channelID)
}

func (conn *Connection) ReadFrame(ctx context.Context, handleMethod MethodHandler) (Thunk, error) {
	thunk, err := conn.rawConn.ReadFrame(ctx, handleMethod)
	if err != nil {
		if err != ErrClientClosed {
			err = fmt.Errorf("error reading frame: %w", err)
		}
		conn.closeSilent()
	}
	return thunk, err
}

func (conn *Connection) Close() {
	err := conn.close()
	if err != nil {
		log.Printf("Error closing connection: %v", err)
	}
}

func (conn *Connection) flushAndClose() {
	err := conn.rawConn.Flush()
	if err != nil {
		log.Printf("Error flushing before closing: %v", err)
	}
	conn.closeSilent()
}

func (conn *Connection) closeSilent() {
	_ = conn.close()
}

func (conn *Connection) close() error {
	return conn.rawConn.Close()
}

func (conn *Connection) handleStarting(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionStartOk:
		err := conn.authStrategy.Check(method)
		if err != nil {
			return nil, err
		}
		err = conn.SendMethod(&amqp.ConnectionTune{
			ChannelMax: maxChannels,
			FrameMax:   maxFrameSize,
			Heartbeat:  conn.heartbeatInterval,
		}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleTuning)
	default:
		return nil, unsupported(method)
	}
}

func unsupported(method amqp.Method) error {
	return connectionError(amqp.NotImplemented, fmt.Sprintf("unexpected method %s", method.Name()), method)
}

func (conn *Connection) handleTuning(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionTuneOk:
		if method.ChannelMax > maxChannels || method.FrameMax > maxFrameSize {
			return nil, errors.New("negotiation failed")
		}

		conn.maxChannels = method.ChannelMax
		conn.maxFrameSize = method.FrameMax

		if method.Heartbeat > 0 {
			if method.Heartbeat < conn.heartbeatInterval {
				conn.heartbeatInterval = method.Heartbeat
			}
			// TODO(bilus): Replace with a lock-free priority queue or similar instead of using up an extra go routine per connection.
			go conn.heartbeat()
		}
		return conn.ReadFrame(ctx, conn.handleTuned)
	default:
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleTuned(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionOpen:
		log.Printf("VHost: %s", method.VirtualHost)
		err := conn.SendMethod(&amqp.ConnectionOpenOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	default:
		// TODO(bilus): Extract to HandleMethod.
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleOpen(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ChannelOpen:
		err := conn.SendMethod(&amqp.ChannelOpenOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	case *amqp.ChannelClose:
		err := conn.SendMethod(&amqp.ChannelCloseOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	case *amqp.ChannelCloseOk:
		return conn.ReadFrame(ctx, conn.handleOpen)
	case *amqp.ConnectionClose:
		err := conn.SendMethod(&amqp.ConnectionCloseOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return nil, nil
	case *amqp.QueueDeclare:
		// Only exclusive, non-durable queues with names generated by the server are supported.
		if method.Queue != "" || !method.Exclusive || method.Durable {
			return conn.handleOpenPreconditionFailed(ctx, channelID, "unsupported queue configuration", method)
		}
		if conn.declaredQueue != "" {
			return conn.handleOpenPreconditionFailed(ctx, channelID, "only one queue per connection is allowed", method)
		}

		conn.declaredQueue = shortuuid.New()
		ok := amqp.QueueDeclareOk{
			Queue: conn.declaredQueue,
		}

		err := conn.SendMethod(&ok, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	case *amqp.QueueDelete:
		if conn.declaredQueue == "" || conn.declaredQueue != method.Queue {
			return conn.handleOpenPreconditionFailed(ctx, channelID, "no such queue", method)
		}
		conn.declaredQueue = ""

		err := conn.SendMethod(&amqp.QueueDeleteOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)

	case *amqp.QueueBind:
		if conn.declaredQueue == "" || conn.declaredQueue != method.Queue {
			return conn.handleOpenPreconditionFailed(ctx, channelID, "no such queue", method)
		}
		err := conn.SendMethod(&amqp.QueueBindOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	case *amqp.ExchangeDeclare:
		// TODO: Ignores exchange configuration, e.g. durable, auto-delete.
		if method.Type != "topic" {
			return conn.handleOpenPreconditionFailed(ctx, channelID, "unsupported exchange kind", method)
		}
		err := conn.SendMethod(&amqp.ExchangeDeclareOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleOpen)
	default:
		// TODO(bilus): Extract to HandleMethod.
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleOpenPreconditionFailed(ctx context.Context, channelID ChannelID, msg string, method amqp.Method) (Thunk, error) {
	return func() (Thunk, error) {
		return conn.ReadFrame(ctx, conn.handleOpen)
	}, channelError(channelID, amqp.PreconditionFailed, msg, method)
}

func (conn *Connection) die() {
	atomic.StoreInt32(&conn.dead, 1)
}

// IsDead returns true if connection is closed.
func (conn *Connection) IsDead() bool {
	return atomic.LoadInt32(&conn.dead) != 0
}

func (conn *Connection) handleClosing(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionCloseOk:
		return nil, nil
	default:
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleError(ctx context.Context, channelID ChannelID, amqpErr *amqp.Error) (Thunk, error) {
	var err error
	var handleMethod MethodHandler
	switch amqpErr.ErrorType {
	case amqp.ErrorOnChannel:
		handleMethod = conn.handleOpen
		err = conn.SendMethod(&amqp.ChannelClose{
			ReplyCode: amqpErr.ReplyCode,
			ReplyText: amqpErr.ReplyText,
			ClassID:   amqpErr.ClassID,
			MethodID:  amqpErr.MethodID,
		}, channelID)
	case amqp.ErrorOnConnection:
		handleMethod = conn.handleClosing
		err = conn.SendMethod(&amqp.ConnectionClose{
			ReplyCode: amqpErr.ReplyCode,
			ReplyText: amqpErr.ReplyText,
			ClassID:   amqpErr.ClassID,
			MethodID:  amqpErr.MethodID,
		}, 0)
	default:
		err = fmt.Errorf("internal error: no handler for AMQP error type %d", amqpErr.ErrorType)
	}
	return func() (Thunk, error) {
		return conn.ReadFrame(ctx, handleMethod)
	}, err
}

func handleMethodRejectAll(_ context.Context, _ ChannelID, method amqp.Method) (Thunk, error) {
	return nil, unsupported(method)
}

func (conn *Connection) heartbeat() {
	interval := time.Duration(conn.heartbeatInterval) * time.Second
	intervalMilli := interval.Milliseconds()
	for {
		if conn.IsDead() {
			return
		}
		timeLeft := intervalMilli - time.Now().UnixMilli() - conn.rawConn.LastWriteUnixMilli()
		if timeLeft <= 0 {
			if err := conn.rawConn.Heartbeat(); err != nil {
				return
			}
			conn.sleepUntilDead(interval)
		} else {
			conn.sleepUntilDead(time.Duration(timeLeft) * time.Millisecond)
		}
	}
}

func (conn *Connection) sleepUntilDead(interval time.Duration) {
	start := time.Now()
	for {
		time.Sleep(time.Millisecond)
		if conn.IsDead() {
			return
		}
		if time.Since(start) > interval {
			return
		}
	}
}

func (conn *Connection) DeliverMessage(m Message, exchangeName string, channelID ChannelID) error {
	// TODO(bilus): Split into frames or calculate real max size.
	bodySize := uint64(len(m.Body))
	if bodySize > uint64(conn.maxFrameSize-1024) { // TODO: This isn't what it is.
		return errors.New("message payload too big")
	}

	method := amqp.BasicDeliver{
		ConsumerTag: "",
		DeliveryTag: 123, // TODO(bilus): Figure out this nack/ack thing.
		Redelivered: false,
		Exchange:    exchangeName,
		RoutingKey:  m.RoutingKey,
	}

	ctype := "text/plain"
	header := amqp.ContentHeader{
		BodySize: bodySize,
		ClassID:  amqp.ClassBasic,
		Weight:   0,
		PropertyList: &amqp.BasicPropertyList{
			ContentType: &ctype,
			Timestamp:   &m.Timestamp,
		},
	}
	bodyFrame := &amqp.Frame{Type: byte(amqp.FrameBody), ChannelID: channelID, Payload: m.Body, CloseAfter: false, Sync: true}
	body := []*amqp.Frame{bodyFrame}

	// log.Printf("[DEBUG] Delivering message to %d consumer(s)", len(conn.getConsumers()))
	for _, consumer := range conn.getConsumers(m.RoutingKey) {
		method.ConsumerTag = consumer.tag
		if err := conn.rawConn.sendContent(&method, &header, body, consumer.channelID); err != nil {
			return err
		}
	}
	return nil
}

func (conn *Connection) getConsumers(routingKey string) []*consumer {
	return nil
}
