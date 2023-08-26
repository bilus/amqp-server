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

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
)

// Connection implements the state machine for an AMQP connection.
type Connection struct {
	rawConn           *RawConnection
	heartbeatInterval uint16
	maxChannels       uint16
	maxFrameSize      uint32

	dead int32

	// TODO(bilus): Idea for later: track channel open/closed state using roaring bitmap.
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

func NewConnection(input io.Reader, output io.Writer, closer io.Closer) *Connection {
	return &Connection{
		rawConn:           NewRawConnection(input, output, closer),
		heartbeatInterval: defaultHeartbeatInterval,
		maxChannels:       maxChannels,
		maxFrameSize:      maxFrameSize,
	}
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
			amqpError := AmqpError{}
			if errors.As(err, &amqpError) {
				// Protocol error.
				thunk, err = conn.handleError(ctx, controlChannelID, amqpError.amqpErr)
				if err != nil {
					conn.Close()
					return
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

		var saslData auth.SaslData
		var err error
		if saslData, err = auth.ParsePlain(method.Response); err != nil {
			return nil, AmqpError{amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())}
		}
		_ = saslData

		if method.Mechanism != auth.SaslPlain {
			conn.Close()
		}

		// if !channel.server.checkAuth(saslData) {
		// 	return amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())
		// }
		// conn.userName = saslData.Username
		// conn.clientProperties = method.ClientProperties

		// @todo Send HeartBeat 0 cause not supported yet
		err = conn.SendMethod(&amqp.ConnectionTune{
			ChannelMax: maxChannels,
			FrameMax:   maxFrameSize,
			Heartbeat:  conn.heartbeatInterval,
		}, channelID)
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx, conn.handleStarted)
	default:
		return nil, unsupported(method)
	}
}

func unsupported(method amqp.Method) error {
	return AmqpError{amqp.NewConnectionError(amqp.NotImplemented, fmt.Sprintf("unexpected method %s", method.Name()), method.ClassIdentifier(), method.MethodIdentifier())}
}

func (conn *Connection) handleStarted(ctx context.Context, channelID ChannelID, method amqp.Method) (Thunk, error) {
	// See the state diagram at doc/states.png
	// In the future, an alternative transition to Securing could be here.
	return conn.handleTuning(ctx, channelID, method)
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
	case *amqp.ConnectionClose:
		err := conn.SendMethod(&amqp.ConnectionCloseOk{}, channelID)
		if err != nil {
			return nil, err
		}
		return nil, nil
	default:
		// TODO(bilus): Extract to HandleMethod.
		return nil, unsupported(method)
	}
}

func (conn *Connection) die() {
	atomic.StoreInt32(&conn.dead, 1)
}

func (conn *Connection) isDead() bool {
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
		// TODO(bilus): This is temporary. Figure out channels next.
		handleMethod = handleMethodRejectAll
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
		}, channelID)
	default:
		err = fmt.Errorf("internal error: no handler for AMQP error type %d", amqpErr.ErrorType)
	}
	if err != nil {
		return nil, err
	}
	return conn.ReadFrame(ctx, handleMethod)
}

func handleMethodRejectAll(_ context.Context, _ ChannelID, method amqp.Method) (Thunk, error) {
	return nil, unsupported(method)
}

func (conn *Connection) heartbeat() {
	interval := time.Duration(conn.heartbeatInterval) * time.Second
	intervalMilli := interval.Milliseconds()
	for {
		if conn.isDead() {
			return
		}
		timeLeft := intervalMilli - time.Now().UnixMilli() - conn.rawConn.LastWriteUnixMilli()
		if timeLeft <= 0 {
			if err := conn.rawConn.Heartbeat(); err != nil {
				return
			}
			time.Sleep(interval)
		} else {
			time.Sleep(time.Duration(timeLeft) * time.Millisecond)
		}
	}
}
