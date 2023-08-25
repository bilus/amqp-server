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
	rawConn      *RawConnection
	handleMethod MethodHandler
}

func NewConnection(input io.Reader, output io.Writer, closer io.Closer) *Connection {
	return &Connection{
		rawConn:      NewRawConnection(input, output, closer),
		handleMethod: handleMethodRejectAll,
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
				if err := conn.handleError(amqpError.amqpErr); err != nil {
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
	err := conn.rawConn.SendMethod(method, 0)
	if err == ErrCloseAfter {
		conn.Close()
	}
	return err
}

func (conn *Connection) ReadFrame(ctx context.Context) (thunk, error) {
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

func (conn *Connection) handleStarting(ctx context.Context, method amqp.Method) (thunk, error) {
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
		const maxChannels = 1
		const maxFrameSize = 65536
		const heartbeatInterval = 10
		err = conn.SendMethod(&amqp.ConnectionTune{
			ChannelMax: maxChannels,
			FrameMax:   maxFrameSize,
			Heartbeat:  heartbeatInterval,
		})
		if err != nil {
			return nil, err
		}
		// conn.handleMethod = handleTune

		return nil, nil
	default:
		return nil, AMQPError{amqp.NewConnectionError(amqp.NotImplemented, fmt.Sprintf("unexpected method %s", method.Name()),
			method.ClassIdentifier(), method.MethodIdentifier())}
	}
}

func (conn *Connection) handleError(err *amqp.Error) error {
	switch err.ErrorType {
	case amqp.ErrorOnChannel:
		return conn.SendMethod(&amqp.ChannelClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassID:   err.ClassID,
			MethodID:  err.MethodID,
		})
	case amqp.ErrorOnConnection:
		conn.handleMethod = handleMethodRejectAll
		return conn.SendMethod(&amqp.ConnectionClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassID:   err.ClassID,
			MethodID:  err.MethodID,
		})
	default:
		return fmt.Errorf("internal error: no handler for AMQP error type %d", err.ErrorType)
	}
}

func handleMethodRejectAll(context.Context, amqp.Method) (thunk, error) {
	return nil, AMQPError{amqp.NewConnectionError(amqp.FrameError, "unexpected method, connection not started", 0, 0)}
}
