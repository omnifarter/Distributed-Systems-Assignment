package Pset1

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	NUMBER_OF_CLIENTS = 4
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

type Server struct {
	ClientsReceiveChannel []chan Message
	ClientsSendChannel    []chan Message
	aggChannel            chan Message
}

/*
Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
*/
func (s Server) Broadcast(message string, sender int) {
	for i, ch := range s.ClientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == sender {
			continue
		}
		fmt.Printf("Server  : broadcasting to client %v \n", i)
		ch <- Message{sender, message}
	}
}

/*
Takes the latest message from aggChannel and consumes it.
*/
func (s Server) GetMessages() Message {
	msg := <-s.aggChannel

	fmt.Printf("Server  : Received a message from client %v \n", msg.clientId)
	return msg
}

/*
Starts an instance of a Server.
*/
func StartServer(clientCount int) Server {

	fmt.Println("starting server...")

	var ClientsReceiveChannel []chan Message
	var ClientsSendChannel []chan Message

	for i := 0; i < clientCount; i++ {
		ClientsReceiveChannel = append(ClientsReceiveChannel, make(chan Message))
		ClientsSendChannel = append(ClientsSendChannel, make(chan Message))

	}

	server := Server{ClientsReceiveChannel, ClientsSendChannel, make(chan Message)}

	/*
		spawn a child process for each client receive channel
		once a message is received, it is fed to aggChannnel for broadcasting
	*/

	for _, ch := range server.ClientsSendChannel {
		go func(c chan Message) {
			for {
				server.aggChannel <- <-c
			}
		}(ch)
	}

	// spawn a child process to get message and broadcast
	go func() {
		for {
			msg := server.GetMessages()

			server.Broadcast(msg.message, msg.clientId)
		}
	}()
	return server
}

/*
Simulates sending a message to the sever.
*/
func (c Client) PingServer() bool {
	fmt.Printf("client %v: I'm pinging a message to the server! \n", c.id)
	message := fmt.Sprintf("Client %v: Hey server, I have a message for you!", c.id)

	c.sendChannel <- Message{c.id, message}

	return true
}

/*
Starts an instance of a Client.
*/
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

/*
Start the simulation
WaitGroup is used here to keep the simulation running indefinitely.
*/
func Part1_1() {
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating client server architecture...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)

	server := StartServer(NUMBER_OF_CLIENTS)

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		StartClient(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i])
	}

	wg.Wait()
}
