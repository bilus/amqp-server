package amqp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/valinurovam/garagemq/amqp"
)

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

var (
	ErrCloseAfter = errors.New("close-after frame")
	ErrClosed     = errors.New("EOF")
)

// RawConnection implements basic methods for sending and receiving and parsing frames.
type RawConnection struct {
	input  io.Reader
	output io.Writer
	closer io.Closer
}

type (
	MethodHandler func(context.Context, amqp.Method) *amqp.Error
	ErrorHandler  func(err *amqp.Error) error
)

func NewRawConnection(input io.Reader, output io.Writer, closer io.Closer) *RawConnection {
	conn := &RawConnection{
		input:  input,
		output: output,
		closer: closer,
	}
	return conn
}

func (conn *RawConnection) RequireProtocolHeader() error {
	buf := make([]byte, 8)
	_, err := conn.input.Read(buf)
	if err != nil {
		return fmt.Errorf("error reading protocol header: %w", err)
	}
	// If the server cannot support the protocol specified in the protocol header,
	// it MUST respond with a valid protocol header and then close the socket connection.
	// The client MUST start a new connection by sending a protocol header
	if !bytes.Equal(buf, amqp.AmqpHeader) {
		_, _ = conn.output.Write(amqp.AmqpHeader)
		return errors.New("unsupported protocol")
	}
	return nil
}

func (conn *RawConnection) Close() error {
	return conn.closer.Close()
}

func (conn *RawConnection) SendFrame(frame *amqp.Frame) error {
	// TODO(bilus): Move to Connection.
	buffer := bufio.NewWriterSize(conn.output, 128<<10)
	if err := amqp.WriteFrame(buffer, frame); err != nil && !conn.isClosedError(err) {
		return fmt.Errorf("error writing frame: %w", err)
	}
	if frame.CloseAfter {
		if err := buffer.Flush(); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error writing frame: %w", err)
		}
		return ErrCloseAfter
	}
	if frame.Sync {
		if err := buffer.Flush(); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error writing frame: %w", err)
		}
	} else {
		if err := conn.maybeFlush(buffer); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error writing frame: %w", err)
		}
	}
	return nil
}

func (conn *RawConnection) isClosedError(err error) bool {
	// See: https://github.com/golang/go/issues/4373
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

func (conn *RawConnection) maybeFlush(buffer *bufio.Writer) error {
	if buffer.Buffered() >= flushThreshold {
		return buffer.Flush()
	}
	return nil
}

func (conn *RawConnection) SendMethod(method amqp.Method, channelId uint16) error {
	// TODO(bilus): Use buffer pool.
	buffer := bytes.NewBuffer(make([]byte, 0, 0))
	if err := amqp.WriteMethod(buffer, method, protoVersion); err != nil {
		return fmt.Errorf("error write method: %w", err)
	}
	closeAfter := method.ClassIdentifier() == amqp.ClassConnection && method.MethodIdentifier() == amqp.MethodConnectionCloseOk
	payload := make([]byte, buffer.Len())
	copy(payload, buffer.Bytes()) // Unnecessary now, will be necessary when we use buffer pool.
	return conn.SendFrame(&amqp.Frame{Type: byte(amqp.FrameMethod), ChannelID: channelId, Payload: payload, CloseAfter: closeAfter, Sync: method.Sync()})
}

// ReadFrame reads a frame and invokes handleMethod if it's a method or handleError for protocol errors.
// It returns error for unrecoverable errors such as errors reading data to signify that the protocol flow
// cannot be continued and the connection is or must be closed.
func (conn *RawConnection) ReadFrame(ctx context.Context, handleMethod MethodHandler, handleError ErrorHandler) error {
	buffer := bufio.NewReaderSize(conn.input, 128<<10)
	frame, err := amqp.ReadFrame(buffer)
	if err != nil {
		if err.Error() == "EOF" && !conn.isClosedError(err) {
			return fmt.Errorf("error reading frame: %w", err)
		} else {
			return ErrClosed
		}
	}
	log.Printf("> %v", frame.Type)
	switch frame.Type {
	case amqp.FrameHeartbeat:
		// TODO(bilus): Handle heartbeat.
	case amqp.FrameMethod:
		// TODO(bilus): Reuse buffer or use pool.
		buffer := bytes.NewReader([]byte{})
		buffer.Reset(frame.Payload)
		method, amqpErr := amqp.ReadMethod(buffer, protoVersion)
		log.Printf("Incoming method <- %s", method.Name())
		if amqpErr != nil {
			log.Printf("Error handling frame: %v", amqpErr)
			return handleError(amqp.NewConnectionError(amqp.FrameError, amqpErr.Error(), 0, 0))
		}

		if amqpErr := handleMethod(ctx, method); amqpErr != nil {
			return handleError(amqpErr)
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
	return nil
}
