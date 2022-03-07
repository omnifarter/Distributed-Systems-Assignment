package Pset1

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
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

func getClock(text string) int {
	i, _ := strconv.Atoi(strings.Split((strings.Split(text, "[")[1]), "]")[0])
	return i
}
func (e *EventsProvider) printTotalOrder() {
	//sort first
	sort.Slice(e.printArray, func(i, j int) bool {
		return getClock(e.printArray[i]) < getClock(e.printArray[j])
	})
	fmt.Println("-----TOTAL ORDER-----")
	for _, text := range e.printArray {
		fmt.Println(text)
	}
}

func (e *EventsProvider) addText(text string) {
	e.mu.Lock()
	e.printArray = append(e.printArray, text)
	e.mu.Unlock()
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
	text := fmt.Sprintf("Server  [%v]: broadcasting.", s.LogicalClock)
	fmt.Println(text)
	eventsProvider.addText(text)
	for i, ch := range s.ClientsReceiveChannel {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		if i == message.clientId {
			continue
		}
		updatedMessage := s.setMaxLogicalClock(message)
		ch <- updatedMessage
	}
	eventsProvider.incrementCount()
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
				text := fmt.Sprintf("Server  [%v]: Received a message from client %v", server.LogicalClock, message.clientId)
				fmt.Println(text)
				eventsProvider.addText(text)
				server.aggChannel <- message
				eventsProvider.incrementCount()
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
	text := fmt.Sprintf("client %v[%v]: pinging server", c.id, c.LogicalClock)
	fmt.Println(text)
	eventsProvider.addText(text)

	message := MessageLogicalClock{
		c.id,
		fmt.Sprintf("Client %v[%v]: Ping server", c.id, c.LogicalClock),
		c.LogicalClock,
	}

	c.sendChannel <- message
	eventsProvider.incrementCount()

	return true
}

/*
Starts an instance of a ClientLogicalClock.
*/
func StartClientLogicalClock(clientId int, sendChannel chan MessageLogicalClock, receiveChannel chan MessageLogicalClock) {

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

			text := fmt.Sprintf("Client %v[%v]: message received. %v", client.id, client.LogicalClock, msg.message)
			fmt.Println(text)
			eventsProvider.addText(text)
			eventsProvider.incrementCount()
		}
	}()
}

/*
Start the simulation with the required number of clients.
*/
func Part1_2() {
	rand.Seed(0) // we keep a constant seed so that results are easier to intepret.
	wg := sync.WaitGroup{}
	wg.Add(1)

	textArray := make([]string, 0)
	eventsProvider = &EventsProvider{0, &sync.Mutex{}, &wg, textArray}

	fmt.Println("Simulating lampart's logical clock...")

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

	eventsProvider.printTotalOrder()
}
