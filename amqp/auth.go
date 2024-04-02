package amqp

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/auth"
)

type AuthStrategy interface {
	Check(method *amqp.ConnectionStartOk) error
}

type NoAuth struct{}

func (n NoAuth) Check(method *amqp.ConnectionStartOk) error {
	return nil
}

type User struct {
	Username string
	Password string
}

type SimpleAuth struct {
	Users map[string]User
}

func NewSimpleAuth(users ...User) *SimpleAuth {
	sa := &SimpleAuth{
		Users: make(map[string]User),
	}
	for _, user := range users {
		sa.Users[user.Username] = user
	}
	return sa
}

func (s SimpleAuth) Check(method *amqp.ConnectionStartOk) error {
	var saslData auth.SaslData
	var err error
	if method.Mechanism != auth.SaslPlain {
		return connectionError(amqp.NotAllowed, "unsupported auth method", method)
	}
	if saslData, err = auth.ParsePlain(method.Response); err != nil {
		return connectionError(amqp.NotAllowed, "login failure", method)
	}
	user, ok := s.Users[saslData.Username]
	if !ok || user.Password != saslData.Password {
		return connectionError(amqp.NotAllowed, "login failure", method)
	}
	return nil
}
