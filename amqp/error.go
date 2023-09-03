package amqp

import "github.com/valinurovam/garagemq/amqp"

type amqpError struct {
	amqpErr   *amqp.Error
	ChannelID uint16
}

func (err amqpError) Error() string {
	return err.amqpErr.ReplyText
}

func connectionError(code uint16, text string, method amqp.Method) amqpError {
	return amqpError{
		amqpErr: amqp.NewConnectionError(
			code,
			text,
			classIdentifier(method),
			methodIdentifier(method),
		),
		ChannelID: controlChannelID,
	}
}

func channelError(channelID uint16, code uint16, text string, method amqp.Method) amqpError {
	return amqpError{
		amqpErr: amqp.NewChannelError(
			code,
			text,
			classIdentifier(method),
			methodIdentifier(method)),
		ChannelID: channelID,
	}
}

func classIdentifier(method amqp.Method) uint16 {
	if method != nil {
		return method.ClassIdentifier()
	}
	return 0
}

func methodIdentifier(method amqp.Method) uint16 {
	if method != nil {
		return method.MethodIdentifier()
	}
	return 0
}
