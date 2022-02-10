package Pset1

import (
	"fmt"
	"math/rand"
	"time"
)

type Client struct {
	id             int
	sendChannel    chan Message
	receiveChannel chan Message
}
type Message struct {
	clientId int
	message  string
}

// Simulates sending a message to the sever.
func (c Client) PingServer() bool {
	fmt.Printf("client %v: I'm pinging a message to the server! \n", c.id)
	message := fmt.Sprintf("Client %v: Hey server, I have a message for you!", c.id)

	c.sendChannel <- Message{c.id, message}

	return true
}

func StartClient(clientId int, sendChannel chan Message, receiveChannel chan Message) {
	fmt.Printf("starting client %v ... \n", clientId)

	client := Client{clientId, sendChannel, receiveChannel}

	// spawn child process to ping server
	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
			client.PingServer()
		}
	}()

	// spawn child process to listen and consume messages on channel
	go func() {
		for {
			msg := <-client.receiveChannel

			fmt.Printf("Client %v: I received a message from the server, message was from client %v! \n", client.id, msg.clientId)

		}
	}()
}
