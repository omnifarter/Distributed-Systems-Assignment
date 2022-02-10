package Pset1

import (
	"fmt"
	"math/rand"
	"time"
)

type Server struct {
	clientsReceiveChannel []chan Message
	clientsSendChannel    []chan Message
	aggChannel            chan Message
}

// Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
func (s Server) broadcast(message string, sender int) {
	for i, ch := range s.clientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == sender {
			continue
		}
		fmt.Printf("Server: broadcasting to client %v \n", i)
		ch <- Message{sender, message}
	}
}

func (s Server) GetMessages() (Message, error) {
	msg := <-s.aggChannel

	fmt.Printf("Server: Received a message from client %v \n", msg.clientId)
	return msg, nil
}

func StartServer(clientCount int) (Server, error) {

	fmt.Println("starting server...")

	var clientsReceiveChannel []chan Message
	var clientsSendChannel []chan Message

	for i := 0; i < clientCount; i++ {
		clientsReceiveChannel = append(clientsReceiveChannel, make(chan Message))
		clientsSendChannel = append(clientsSendChannel, make(chan Message))

	}

	server := Server{clientsReceiveChannel, clientsSendChannel, make(chan Message)}

	/*
		spawn a child process for each client receive channel
		once a message is received, it is fed to aggChannnel for broadcasting
	*/

	for _, ch := range server.clientsSendChannel {
		go func(c chan Message) {
			for {
				server.aggChannel <- <-c
			}
		}(ch)
	}

	// spawn a child process to get message and broadcast
	go func() {
		for {
			msg, err := server.GetMessages()

			if err != nil {
				fmt.Println(err)
				break
			}
			server.broadcast(msg.message, msg.clientId)
		}
	}()
	return server, nil
}
