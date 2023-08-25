package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strings"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
)

// Step 1. Have it accept connections.

func main() {
	ctx := context.Background()
	server := Server{}
	err := server.Start(ctx)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

type Server struct{}

func (s *Server) Start(ctx context.Context) error {
	address := "localhost:5672"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	log.Println("Server started")

	for {
		netConn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		netConn.SetReadBuffer(196608)
		netConn.SetWriteBuffer(196608)
		netConn.SetNoDelay(false)

		conn := Connection{netConn: netConn}
		conn.handleMethod = conn.handleConnecting
		conn.handle(ctx)
	}
}

// From https://github.com/rabbitmq/rabbitmq-common/blob/master/src/rabbit_writer.erl
// When the amount of protocol method data buffered exceeds
// this threshold, a socket flush is performed.
//
// This magic number is the tcp-over-ethernet MSS (1460) minus the
// minimum size of a AMQP 0-9-1 basic.deliver method frame (24) plus basic
// content header (22). The idea is that we want to flush just before
// exceeding the MSS.
const flushThreshold = 1414

const protoVersion = "amqp-rabbit"

type Connection struct {
	netConn *net.TCPConn
	// States:
	//
	handleMethod func(context.Context, amqp.Method) *amqp.Error
}

func (conn *Connection) handle(ctx context.Context) {
	buf := make([]byte, 8)
	_, err := conn.netConn.Read(buf)
	if err != nil {
		log.Println("Error reading protocol header")
		conn.close()
		return
	}
	// If the server cannot support the protocol specified in the protocol header,
	// it MUST respond with a valid protocol header and then close the socket connection.
	// The client MUST start a new connection by sending a protocol header
	if !bytes.Equal(buf, amqp.AmqpHeader) {
		log.Println("Unsupported protocol")
		_, _ = conn.netConn.Write(amqp.AmqpHeader)
		conn.close()
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
	conn.sendMethod(&method, 0)

	log.Println("Reading frame")

	conn.handleMethod = conn.handleConnected

	conn.readFrame(ctx)
}

func (conn *Connection) handleConnecting(context.Context, amqp.Method) *amqp.Error {
	return amqp.NewConnectionError(amqp.FrameError, "unexpected method, connection not started", 0, 0)
}

func (conn *Connection) handleClosing(context.Context, amqp.Method) *amqp.Error {
	return amqp.NewConnectionError(amqp.FrameError, "unexpected method, connection not started", 0, 0)
}

func (conn *Connection) handleConnected(ctx context.Context, method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.ConnectionStartOk:

		var saslData auth.SaslData
		var err error
		if saslData, err = auth.ParsePlain(method.Response); err != nil {
			return amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())
		}
		_ = saslData

		if method.Mechanism != auth.SaslPlain {
			conn.close()
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
		conn.sendMethod(&amqp.ConnectionTune{
			ChannelMax: maxChannels,
			FrameMax:   maxFrameSize,
			Heartbeat:  heartbeatInterval,
		}, 0)
		// conn.handleMethod = handleTune

		return nil
	default:
		return amqp.NewConnectionError(amqp.NotImplemented, fmt.Sprintf("unexpected method %s", method.Name()),
			method.ClassIdentifier(), method.MethodIdentifier())
	}
}

func (conn *Connection) close() {
	err := conn.netConn.Close()
	if err != nil {
		log.Printf("Error closing connection: %v", err)
	}
}

func (conn *Connection) sendFrame(frame *amqp.Frame) {
	// TODO(bilus): Move to connection.
	buffer := bufio.NewWriterSize(conn.netConn, 128<<10)
	if err := amqp.WriteFrame(buffer, frame); err != nil && !conn.isClosedError(err) {
		log.Printf("Error writing frame: %v", err)
		conn.close()
		return
	}
	if frame.CloseAfter {
		if err := buffer.Flush(); err != nil && !conn.isClosedError(err) {
			log.Printf("Error writing frame: %v", err)
		}
		conn.close()
		return
	}
	if frame.Sync {
		if err := buffer.Flush(); err != nil && !conn.isClosedError(err) {
			log.Printf("Error writing frame: %v", err)
			conn.close()
		}
		return
	} else {
		if err := conn.maybeFlush(buffer); err != nil && !conn.isClosedError(err) {
			log.Printf("Error writing frame: %v", err)
			conn.close()
		}
		return

	}
}

func (conn *Connection) isClosedError(err error) bool {
	// See: https://github.com/golang/go/issues/4373
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

func (conn *Connection) maybeFlush(buffer *bufio.Writer) error {
	if buffer.Buffered() >= flushThreshold {
		return buffer.Flush()
	}
	return nil
}

func (conn *Connection) sendMethod(method amqp.Method, channelId uint16) {
	// TODO(bilus): Use buffer pool.
	buffer := bytes.NewBuffer(make([]byte, 0, 0))
	if err := amqp.WriteMethod(buffer, method, protoVersion); err != nil {
		log.Printf("Error write method: %v", err)
		return
	}
	closeAfter := method.ClassIdentifier() == amqp.ClassConnection && method.MethodIdentifier() == amqp.MethodConnectionCloseOk
	payload := make([]byte, buffer.Len())
	copy(payload, buffer.Bytes()) // Unnecessary now, will be necessary when we use buffer pool.
	conn.sendFrame(&amqp.Frame{Type: byte(amqp.FrameMethod), ChannelID: channelId, Payload: payload, CloseAfter: closeAfter, Sync: method.Sync()})
}

func (conn *Connection) readFrame(ctx context.Context) {
	log.Println("readFrame")
	buffer := bufio.NewReaderSize(conn.netConn, 128<<10)
	frame, err := amqp.ReadFrame(buffer)
	if err != nil {
		log.Println(err)
		if err.Error() != "EOF" && !conn.isClosedError(err) {
			log.Printf("Error reading frame: %v", err)
		}
		return
	}
	log.Printf("> %v", frame.Type)
	switch frame.Type {
	case amqp.FrameHeartbeat:
		// TODO(bilus): Handle heartbeat.
	case amqp.FrameMethod:
		log.Printf("FrameMethod")
		// TODO(bilus): Reuse buffer or use pool.
		buffer := bytes.NewReader([]byte{})
		buffer.Reset(frame.Payload)
		method, err := amqp.ReadMethod(buffer, protoVersion)
		log.Printf("Incoming method <- %s", method.Name())
		if err != nil {
			log.Printf("Error handling frame: %v", err)
			conn.sendError(amqp.NewConnectionError(amqp.FrameError, err.Error(), 0, 0))
		}

		if err := conn.handleMethod(ctx, method); err != nil {
			conn.sendError(err)
		}
	case amqp.FrameHeader:
		// if err := channel.handleContentHeader(frame); err != nil {
		// 	channel.sendError(err)
		// }
	case amqp.FrameBody:
		// if err := channel.handleContentBody(frame); err != nil {
		// 	channel.sendError(err)
		// }
	}
}

func (conn *Connection) sendError(err *amqp.Error) {
	switch err.ErrorType {
	case amqp.ErrorOnChannel:
		conn.sendMethod(&amqp.ChannelClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassID:   err.ClassID,
			MethodID:  err.MethodID,
		}, 0)
	case amqp.ErrorOnConnection:
		conn.handleMethod = conn.handleClosing
		conn.sendMethod(&amqp.ConnectionClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassID:   err.ClassID,
			MethodID:  err.MethodID,
		}, 0)
	}
}
