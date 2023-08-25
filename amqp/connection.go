package amqp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
)

type Connection struct {
	rawConn           *RawConnection
	handleMethod      MethodHandler
	heartbeatInterval uint16
	maxChannels       uint16
	maxFrameSize      uint32
}

const (
	maxChannels              = 1
	maxFrameSize             = 65536
	defaultHeartbeatInterval = 10
)

func NewConnection(input io.Reader, output io.Writer, closer io.Closer) *Connection {
	return &Connection{
		rawConn:           NewRawConnection(input, output, closer),
		handleMethod:      handleMethodRejectAll,
		heartbeatInterval: defaultHeartbeatInterval,
		maxChannels:       maxChannels,
		maxFrameSize:      maxFrameSize,
	}
}

func (conn *Connection) Start(ctx context.Context) {
	err := conn.rawConn.RequireProtocolHeader()
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
	conn.SendMethod(&method)
	conn.handleMethod = conn.handleStarting

	thunk, err := conn.ReadFrame(ctx)
	for {
		if err != nil {
			amqpError := AMQPError{}
			if errors.As(err, &amqpError) {
				// Protocol error.
				thunk, err = conn.handleError(ctx, amqpError.amqpErr)
				if err != nil {
					conn.Close()
					return
				}
			} else {
				log.Printf("Error: %v", err)
			}
		}
		if thunk == nil {
			conn.Close()
			log.Println("Finished")
			return
		}
		thunk, err = thunk()
	}
}

func (conn *Connection) SendMethod(method amqp.Method) error {
	log.Println("=>", method.Name())
	err := conn.rawConn.SendMethod(method, 0)
	if err == ErrCloseAfter {
		conn.Close()
	}
	return err
}

func (conn *Connection) ReadFrame(ctx context.Context) (Thunk, error) {
	thunk, err := conn.rawConn.ReadFrame(ctx, conn.handleMethod)
	if err != nil {
		if err != ErrClosed {
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

func (conn *Connection) closeSilent() {
	_ = conn.close()
}

func (conn *Connection) close() error {
	return conn.rawConn.Close()
}

func (conn *Connection) handleStarting(ctx context.Context, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionStartOk:

		var saslData auth.SaslData
		var err error
		if saslData, err = auth.ParsePlain(method.Response); err != nil {
			return nil, AMQPError{amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())}
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
		})
		if err != nil {
			return nil, err
		}
		conn.handleMethod = conn.handleStarted
		return conn.ReadFrame(ctx)
	default:
		return nil, unsupported(method)
	}
}

func unsupported(method amqp.Method) error {
	return AMQPError{amqp.NewConnectionError(amqp.NotImplemented, fmt.Sprintf("unexpected method %s", method.Name()), method.ClassIdentifier(), method.MethodIdentifier())}
}

func (conn *Connection) handleStarted(ctx context.Context, method amqp.Method) (Thunk, error) {
	// See the state diagram at doc/states.png
	// In the future, an alternative transition to Securing could be here.
	conn.handleMethod = conn.handleTuning
	return conn.handleMethod(ctx, method)
}

func (conn *Connection) handleTuning(ctx context.Context, method amqp.Method) (Thunk, error) {
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
			// TODO(bilus): Implement heartbeats. Do we need a channel?
			// go conn.heartbeat()
		}
		conn.handleMethod = conn.handleTuned
		return conn.ReadFrame(ctx)
	default:
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleTuned(ctx context.Context, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionOpen:
		log.Printf("VHost: %s", method.VirtualHost)
		err := conn.SendMethod(&amqp.ConnectionOpenOk{})
		if err != nil {
			return nil, err
		}
		return conn.ReadFrame(ctx)
	default:
		// TODO(bilus): Extract to HandleMethod.
		return nil, unsupported(method)
	}
}

func (conn *Connection) handleClosing(ctx context.Context, method amqp.Method) (Thunk, error) {
	switch method := method.(type) {
	case *amqp.ConnectionCloseOk:
		return nil, nil
	default:
		return nil, unsupported(method)
	}
}

func (conn *Connection) heartbeatTimeout() uint16 {
	return conn.heartbeatInterval * 3
}

func (conn *Connection) handleError(ctx context.Context, amqpErr *amqp.Error) (Thunk, error) {
	var err error
	switch amqpErr.ErrorType {
	case amqp.ErrorOnChannel:
		// TODO(bilus): This is temporary. Figure out channels next.
		conn.handleMethod = handleMethodRejectAll
		err = conn.SendMethod(&amqp.ChannelClose{
			ReplyCode: amqpErr.ReplyCode,
			ReplyText: amqpErr.ReplyText,
			ClassID:   amqpErr.ClassID,
			MethodID:  amqpErr.MethodID,
		})
	case amqp.ErrorOnConnection:
		conn.handleMethod = conn.handleClosing
		err = conn.SendMethod(&amqp.ConnectionClose{
			ReplyCode: amqpErr.ReplyCode,
			ReplyText: amqpErr.ReplyText,
			ClassID:   amqpErr.ClassID,
			MethodID:  amqpErr.MethodID,
		})
	default:
		err = fmt.Errorf("internal error: no handler for AMQP error type %d", amqpErr.ErrorType)
	}
	if err != nil {
		return nil, err
	}
	return conn.ReadFrame(ctx)
}

func handleMethodRejectAll(context.Context, amqp.Method) (Thunk, error) {
	return nil, AMQPError{amqp.NewConnectionError(amqp.FrameError, "unexpected method, connection not started", 0, 0)}
}
