package Pset1

import (
	"fmt"
	"math/rand"
	"time"
)

type ClientVectorClock struct {
	id             int
	sendChannel    chan MessageVectorClock
	receiveChannel chan MessageVectorClock

	VectorClock []int
}
type MessageVectorClock struct {
	clientId int
	message  string

	VectorClock []int
}

type ServerVectorClock struct {
	ClientsReceiveChannel []chan MessageVectorClock
	ClientsSendChannel    []chan MessageVectorClock
	aggChannel            chan MessageVectorClock

	VectorClock []int // server clock is indexed 0
}

// updates the server's Vector clock if the message contains a later Vector clock
func (s *ServerVectorClock) setMaxVectorClock(message MessageVectorClock) MessageVectorClock {
	for i, clock := range message.VectorClock {
		if i == 0 {
			continue
		} else {
			s.VectorClock[i] = clock
		}

	}

	return MessageVectorClock{message.clientId, message.message, s.VectorClock}
}

// updates the client's Vector clock if the message contains a later Vector clock
func (c *ClientVectorClock) setMaxVectorClock(message MessageVectorClock) MessageVectorClock {

	for i, clock := range message.VectorClock {
		if i == c.id+1 {
			continue
		} else {
			c.VectorClock[i] = clock
		}
	}
	return MessageVectorClock{message.clientId, message.message, c.VectorClock}
}

// Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
func (s *ServerVectorClock) BroadcastVectorClock(message MessageVectorClock) {
	// adding the increment of the Vector clock here signifies that broadcasting is considered 1 event.
	s.VectorClock[0]++
	fmt.Printf("Server   %v: broadcasting. \n", s.VectorClock)
	for i, ch := range s.ClientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == message.clientId {
			continue
		}
		updatedMessage := s.setMaxVectorClock(message)
		ch <- updatedMessage
	}
}

func (s *ServerVectorClock) GetMessagesVectorClock() (MessageVectorClock, error) {
	msg := <-s.aggChannel
	return msg, nil
}

func StartServerVectorClock(clientCount int, vectorClock []int) (ServerVectorClock, error) {
	
	fmt.Println("starting server...")

	var ClientsReceiveChannel []chan MessageVectorClock
	var ClientsSendChannel []chan MessageVectorClock

	for i := 0; i < clientCount; i++ {
		ClientsReceiveChannel = append(ClientsReceiveChannel, make(chan MessageVectorClock))
		ClientsSendChannel = append(ClientsSendChannel, make(chan MessageVectorClock))

	}

	server := ServerVectorClock{
		ClientsReceiveChannel,
		ClientsSendChannel,
		make(chan MessageVectorClock),
		vectorClock,
	}

	/*
		spawn a child process for each client receive channel
		once a message is received, it is fed to aggChannnel for broadcasting
	*/

	for _, ch := range server.ClientsSendChannel {
		go func(c chan MessageVectorClock) {
			for {
				// setting and incrementing Vector clock here signifiies that messages are processed here.
				message := <-c
				message = server.setMaxVectorClock(message)
				server.VectorClock[0]++
				fmt.Printf("Server   %v: Received a message from client %v \n", server.VectorClock, message.clientId)
				server.aggChannel <- message
			}
		}(ch)
	}

	// spawn a child process to get message and broadcast
	go func() {
		for {
			msg, err := server.GetMessagesVectorClock()

			if err != nil {
				fmt.Println(err)
				break
			}
			server.BroadcastVectorClock(msg)
		}
	}()
	return server, nil
}

// Simulates sending a message to the sever.
func (c *ClientVectorClock) PingServerVectorClock() bool {
	c.VectorClock[c.id+1]++
	fmt.Printf("client %v %v: pinging server \n", c.id, c.VectorClock)

	message := MessageVectorClock{
		c.id,
		fmt.Sprintf("Client %v %v: Ping server", c.id, c.VectorClock),
		c.VectorClock,
	}

	c.sendChannel <- message

	return true
}

func StartClientVectorClock(clientId int, sendChannel chan MessageVectorClock, receiveChannel chan MessageVectorClock, vectorClock []int) {
	fmt.Printf("starting client %v ... \n", clientId)

	client := ClientVectorClock{clientId, sendChannel, receiveChannel, vectorClock}

	// spawn child process to ping server
	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
			client.PingServerVectorClock()
		}
	}()

	// spawn child process to listen and consume messages on channel
	go func() {
		for {
			msg := <-client.receiveChannel
			client.setMaxVectorClock(msg)
			client.VectorClock[client.id+1]++
			fmt.Printf("Client %v %v: message received. \n", client.id, client.VectorClock)

		}
	}()
}
