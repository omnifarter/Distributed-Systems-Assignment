package Pset1

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	NUMBER_OF_CLIENTS = 4
	NUMBER_OF_EVENTS  = 10
)

var eventsProvider *EventsProvider

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
This global variable keeps count of the total number of events processed across all machines.
Once count == NUMBER_OF_EVENTS, we exit the program gracefully.
*/
type EventsProvider struct {
	count      int
	mu         *sync.Mutex
	wg         *sync.WaitGroup
	printArray []string
}

func (e *EventsProvider) incrementCount() {
	e.mu.Lock()
	if e.count == NUMBER_OF_EVENTS {
		e.wg.Done()
		return
	} else {
		e.count += 1
	}
	e.mu.Unlock()
}

/*
Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
*/
func (s Server) Broadcast(message string, sender int) {
	fmt.Println("Server  : broadcasting.")
	for i, ch := range s.ClientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == sender {
			continue
		}
		ch <- Message{sender, message}
	}
	eventsProvider.incrementCount()
}

/*
Takes the latest message from aggChannel and consumes it.
*/
func (s Server) GetMessages() Message {
	msg := <-s.aggChannel

	fmt.Printf("Server  : Received a message from client %v \n", msg.clientId)
	eventsProvider.incrementCount()
	return msg
}

/*
Starts an instance of a Server.
*/
func StartServer(clientCount int) Server {

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
	eventsProvider.incrementCount()

	return true
}

/*
Starts an instance of a Client.
*/
func StartClient(clientId int, sendChannel chan Message, receiveChannel chan Message) {

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
			eventsProvider.incrementCount()

		}
	}()
}

/*
Start the simulation
*/
func Part1_1() {
	rand.Seed(0) // we keep a constant seed so that results are easier to intepret.
	wg := sync.WaitGroup{}
	wg.Add(1)
	eventsProvider = &EventsProvider{0, &sync.Mutex{}, &wg, make([]string, 0)}

	fmt.Println("Simulating client server architecture...")
	time.Sleep(time.Second * 2)

	server := StartServer(NUMBER_OF_CLIENTS)

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		StartClient(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i])
	}

	wg.Wait()
}
