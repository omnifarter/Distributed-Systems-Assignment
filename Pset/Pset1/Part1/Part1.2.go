package Pset1

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ClientLogicalClock struct {
	id             int
	sendChannel    chan MessageLogicalClock
	receiveChannel chan MessageLogicalClock

	LogicalClock int
}
type MessageLogicalClock struct {
	clientId int
	message  string

	LogicalClock int
}

type ServerLogicalClock struct {
	ClientsReceiveChannel []chan MessageLogicalClock
	ClientsSendChannel    []chan MessageLogicalClock
	aggChannel            chan MessageLogicalClock

	LogicalClock int
}

/*
Updates the server's logical clock if the message contains a later logical clock
*/
func (s *ServerLogicalClock) setMaxLogicalClock(message MessageLogicalClock) MessageLogicalClock {
	logicalClock := s.LogicalClock

	if logicalClock < message.LogicalClock {
		s.LogicalClock = message.LogicalClock
		logicalClock = message.LogicalClock
	}

	return MessageLogicalClock{message.clientId, message.message, logicalClock}
}

/*
Updates the client's logical clock if the message contains a later logical clock
*/
func (c *ClientLogicalClock) setMaxLogicalClock(message MessageLogicalClock) MessageLogicalClock {
	logicalClock := c.LogicalClock

	if logicalClock < message.LogicalClock {
		c.LogicalClock = message.LogicalClock
		logicalClock = message.LogicalClock
	}

	return MessageLogicalClock{message.clientId, message.message, logicalClock}
}

/*
Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
*/
func (s *ServerLogicalClock) BroadcastLogicalClock(message MessageLogicalClock) {
	// adding the increment of the logical clock here signifies that broadcasting is considered 1 event.
	s.LogicalClock++
	fmt.Printf("Server  [%v]: broadcasting. \n", s.LogicalClock)
	for i, ch := range s.ClientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == message.clientId {
			continue
		}
		updatedMessage := s.setMaxLogicalClock(message)
		ch <- updatedMessage
	}
}

/*
Takes the latest message from aggChannel and consumes it.
*/
func (s *ServerLogicalClock) GetMessagesLogicalClock() (MessageLogicalClock, error) {
	msg := <-s.aggChannel
	return msg, nil
}

/*
Starts an instance of a ServerLogicalClock.
*/
func StartServerLogicalClock(clientCount int) (ServerLogicalClock, error) {

	fmt.Println("starting server...")

	var ClientsReceiveChannel []chan MessageLogicalClock
	var ClientsSendChannel []chan MessageLogicalClock

	for i := 0; i < clientCount; i++ {
		ClientsReceiveChannel = append(ClientsReceiveChannel, make(chan MessageLogicalClock))
		ClientsSendChannel = append(ClientsSendChannel, make(chan MessageLogicalClock))

	}

	server := ServerLogicalClock{
		ClientsReceiveChannel,
		ClientsSendChannel,
		make(chan MessageLogicalClock),
		0,
	}

	/*
		spawn a child process for each client receive channel
		once a message is received, it is fed to aggChannnel for broadcasting
	*/

	for _, ch := range server.ClientsSendChannel {
		go func(c chan MessageLogicalClock) {
			for {
				// setting and incrementing logical clock here signifiies that messages are processed here.
				message := <-c
				message = server.setMaxLogicalClock(message)
				server.LogicalClock++
				fmt.Printf("Server  [%v]: Received a message from client %v \n", server.LogicalClock, message.clientId)
				server.aggChannel <- message
			}
		}(ch)
	}

	// spawn a child process to get message and broadcast
	go func() {
		for {
			msg, err := server.GetMessagesLogicalClock()

			if err != nil {
				fmt.Println(err)
				break
			}
			server.BroadcastLogicalClock(msg)
		}
	}()
	return server, nil
}

/*
Simulates sending a message to the sever.
*/
func (c *ClientLogicalClock) PingServerLogicalClock() bool {
	c.LogicalClock++
	fmt.Printf("client %v[%v]: pinging server \n", c.id, c.LogicalClock)

	message := MessageLogicalClock{
		c.id,
		fmt.Sprintf("Client %v[%v]: Ping server", c.id, c.LogicalClock),
		c.LogicalClock,
	}

	c.sendChannel <- message

	return true
}

/*
Starts an instance of a ClientLogicalClock.
*/
func StartClientLogicalClock(clientId int, sendChannel chan MessageLogicalClock, receiveChannel chan MessageLogicalClock) {
	fmt.Printf("starting client %v ... \n", clientId)

	client := ClientLogicalClock{clientId, sendChannel, receiveChannel, 0}

	// spawn child process to ping server
	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
			client.PingServerLogicalClock()
		}
	}()

	// spawn child process to listen and consume messages on channel
	go func() {
		for {
			msg := <-client.receiveChannel
			client.setMaxLogicalClock(msg)
			client.LogicalClock++
			fmt.Printf("Client %v[%v]: message received. \n", client.id, client.LogicalClock)

		}
	}()
}

/*
Start the simulation with the required number of clients.
*/
func Part1_2() {
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating lampart's logical clock...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)

	server, err := StartServerLogicalClock(NUMBER_OF_CLIENTS)

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		StartClientLogicalClock(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i])
	}

	wg.Wait()

}
