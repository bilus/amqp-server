package amqp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/amqp"
)

// Experimenting.
type bufferedWriter = bufio.Writer

func newBufferedWriter(w io.Writer, size int) *bufferedWriter {
	return bufio.NewWriterSize(w, size)
}

// From https://github.com/rabbitmq/rabbitmq-common/blob/master/src/rabbit_writer.erl
// When the amount of protocol method data buffered exceeds
// this threshold, a socket flush is performed.
//
// This magic number is the tcp-over-ethernet MSS (1460) minus the minimum size
// of a AMQP 0-9-1 basic.deliver method frame (24) plus basic content header
// (22). The idea is that we want to flush just before exceeding the MSS.
const flushThreshold = 1414

const protoVersion = "amqp-rabbit"

var (
	ErrCloseAfter   = errors.New("closing after writing last frame")
	ErrClientClosed = errors.New("EOF")
)

// RawConnection implements basic methods for sending and receiving and parsing frames.
type RawConnection struct {
	input  io.Reader
	output *bufferedWriter
	closer io.Closer

	lastWriteUnixMilli int64

	writeMtx sync.Mutex
}

type (
	ChannelID = uint16

	// MethodHandler handles an AMQP method received by the server. It MUST
	// return an error if any errors occurred during handling of the method. It
	// MAY return a thunk to continue processing (usually a call to ReadFrame)
	// or nil to close the connection. It MAY return both error and thunk for
	// the top-level code to handle the error (e.g. by logging it) but so that
	// processing continues without closing connection (e.g. to retry an
	// operation).
	MethodHandler func(context.Context, ChannelID, amqp.Method) (Thunk, error)
	ErrorHandler  func(err *amqp.Error) error
)

// NewRawConnection creates a new connection.
func NewRawConnection(input io.Reader, output io.Writer, closer io.Closer) *RawConnection {
	conn := &RawConnection{
		input:  bufio.NewReaderSize(input, 123<<10),
		output: newBufferedWriter(output, 128<<10),
		closer: closer,
	}
	return conn
}

// ReadAmqpHeader reads AMQP 0.9.1 header. Otherwise it returns an error but
// only after writing the header back to the client, as dictated by the spec.
func (conn *RawConnection) ReadAmqpHeader() error {
	buf := make([]byte, 8)
	_, err := conn.input.Read(buf)
	if err != nil {
		return fmt.Errorf("error reading protocol header: %w", err)
	}
	// If the server cannot support the protocol specified in the protocol
	// header, it MUST respond with a valid protocol header and then close the
	// socket connection. The client MUST start a new connection by sending a
	// protocol header
	if !bytes.Equal(buf, amqp.AmqpHeader) {
		_, _ = conn.output.Write(amqp.AmqpHeader)
		return errors.New("unsupported protocol")
	}
	return nil
}

// Close terminates the connection.
func (conn *RawConnection) Close() error {
	return conn.closer.Close()
}

// Flush writes out buffer to output.
func (conn *RawConnection) Flush() error {
	return conn.output.Flush()
}

// SendMethod sends AMQP method to the client.
func (conn *RawConnection) SendMethod(method amqp.Method, channelId uint16) error {
	log.Debugf("<= %s chan = %d", method.Name(), channelId)
	// TODO(bilus): Use buffer pool (after benchmarking).
	buffer := bytes.NewBuffer(make([]byte, 0, 0))
	if err := amqp.WriteMethod(buffer, method, protoVersion); err != nil {
		return fmt.Errorf("error write method: %w", err)
	}
	closeAfter := method.ClassIdentifier() == amqp.ClassConnection && method.MethodIdentifier() == amqp.MethodConnectionCloseOk
	payload := make([]byte, buffer.Len())
	copy(payload, buffer.Bytes()) // Unnecessary now, will be necessary when we use buffer pool.
	return conn.sendFrame(&amqp.Frame{Type: byte(amqp.FrameMethod), ChannelID: channelId, Payload: payload, CloseAfter: closeAfter, Sync: method.Sync()})
}

// ReadFrame reads a frame and invokes handleMethod if it's a method or
// handleError for protocol errors. It returns error for unrecoverable errors
// such as errors reading data to signify that the protocol flow cannot be
// continued and the connection is or must be closed.
//
// Note that the method returns a thunk which must be invoked in order for the
// handler to be actually called.
func (conn *RawConnection) ReadFrame(ctx context.Context, handleMethod MethodHandler) (Thunk, error) {
	frame, err := amqp.ReadFrame(conn.input)
	if err != nil {
		if err.Error() != "EOF" && !conn.isClosedError(err) {
			return nil, fmt.Errorf("error reading frame: %w", err)
		} else {
			return nil, ErrClientClosed
		}
	}
	switch frame.Type {
	case amqp.FrameHeartbeat:
		log.Debug("=> Heartbeat")
		// TODO(bilus): Handle client heartbeat.
		return conn.ReadFrame(ctx, handleMethod)
	case amqp.FrameMethod:
		// TODO(bilus): Reuse buffer or use pool. Though most active connections
		// will spend most time waiting for data from client so the pool would
		// grow large anyway and I'm unsure how much we'd gain, esp. as the number
		// of active connections drops.
		buffer := bytes.NewReader([]byte{})
		buffer.Reset(frame.Payload)
		method, amqpErr := amqp.ReadMethod(buffer, protoVersion)
		log.Debugf("=> %s chan = %d", method.Name(), frame.ChannelID)
		if amqpErr != nil {
			// TODO(bilus): It can either be a connection or channel error.
			log.Printf("Error handling frame: %v", amqpErr)
			return nil, connectionError(amqp.FrameError, amqpErr.Error(), nil)
		}

		return func() (Thunk, error) {
			return handleMethod(ctx, frame.ChannelID, method)
		}, nil
	case amqp.FrameHeader:
		// if err := channel.handleContentHeader(frame); err != nil {
		// 	channel.sendError(err)
		// }
	case amqp.FrameBody:
		// if err := channel.handleContentBody(frame); err != nil {
		// 	channel.sendError(err)
		// }
	}
	return nil, nil
}

// Heartbeat sends a heartbeat frame.
func (conn *RawConnection) Heartbeat() error {
	log.Debug("<= Heartbeat")
	heartbeatFrame := &amqp.Frame{Type: byte(amqp.FrameHeartbeat), ChannelID: 0, Payload: []byte{}, CloseAfter: false, Sync: true}
	return conn.sendFrame(heartbeatFrame)
}

func (conn *RawConnection) sendFrame(frame *amqp.Frame) error {
	conn.writeMtx.Lock()
	defer conn.writeMtx.Unlock()

	if err := amqp.WriteFrame(conn.output, frame); err != nil && !conn.isClosedError(err) {
		return fmt.Errorf("error writing frame: %w", err)
	}
	if frame.CloseAfter {
		if err := conn.output.Flush(); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error flushing frame (CloseAfter): %w", err)
		}
		return ErrCloseAfter
	}
	if frame.Sync {
		if err := conn.output.Flush(); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error flushing frame (Sync): %w", err)
		}
	} else {
		if err := conn.maybeFlush(conn.output); err != nil && !conn.isClosedError(err) {
			return fmt.Errorf("error flushing frame: %w", err)
		}
	}
	atomic.StoreInt64(&conn.lastWriteUnixMilli, time.Now().UnixMilli())
	return nil
}

// LastWriteUnixMilli Posix time of the last write in ms.
func (conn *RawConnection) LastWriteUnixMilli() int64 {
	return atomic.LoadInt64(&conn.lastWriteUnixMilli)
}

func (conn *RawConnection) isClosedError(err error) bool {
	// See: https://github.com/golang/go/issues/4373
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

func (conn *RawConnection) maybeFlush(buffer *bufferedWriter) error {
	if buffer.Buffered() >= flushThreshold {
		return buffer.Flush()
	}
	return nil
}

// sendContent send message to consumers.
func (conn *RawConnection) sendContent(method amqp.Method, header *amqp.ContentHeader, body []*amqp.Frame, channelID ChannelID) error {
	if err := conn.SendMethod(method, channelID); err != nil {
		return err
	}

	// TODO(bilus): Use buffer pool.
	rawHeader := bytes.NewBuffer(make([]byte, 0, 0))
	amqp.WriteContentHeader(rawHeader, header, protoVersion)

	payload := make([]byte, rawHeader.Len())
	copy(payload, rawHeader.Bytes())

	if err := conn.sendFrame(&amqp.Frame{Type: byte(amqp.FrameHeader), ChannelID: channelID, Payload: payload, CloseAfter: false}); err != nil {
		return err
	}

	for _, payload := range body {
		payload.ChannelID = channelID
		if err := conn.sendFrame(payload); err != nil {
			return err
		}
	}
	return nil
}
