package main

import (
	"github.com/bilus/amqp-server/amqp"
	"context"
	"log"
	"net"
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

		conn := amqp.NewConnection(netConn, netConn, netConn)
		conn.Start(ctx)
	}
}
