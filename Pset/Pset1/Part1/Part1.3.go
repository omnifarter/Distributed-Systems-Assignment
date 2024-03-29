package Pset1

import (
	"fmt"
	"math/rand"
	"sync"
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

func (e *EventsProvider) printCausalityViolations() {
	fmt.Println("----Potential Causality Violations----")
	for _, v := range e.printArray {
		fmt.Println(v)
	}
}

/*
Updates the server's Vector clock if the message contains a later Vector clock
*/
func (s *ServerVectorClock) setMaxVectorClock(message MessageVectorClock) MessageVectorClock {

	// first checks for causality violation
	s.detectCausalityViolation(message)

	for i, clock := range message.VectorClock {
		if i == 0 {
			continue
		} else {
			s.VectorClock[i] = clock
		}

	}

	return MessageVectorClock{message.clientId, message.message, s.VectorClock}
}

/*
Updates the client's Vector clock if the message contains a later Vector clock
*/
func (c *ClientVectorClock) setMaxVectorClock(message MessageVectorClock) MessageVectorClock {

	// first checks for causality violation
	c.detectCausalityViolation(message)

	for i, clock := range message.VectorClock {
		if i == c.id+1 {
			continue
		} else {
			c.VectorClock[i] = clock
		}
	}

	return MessageVectorClock{message.clientId, message.message, c.VectorClock}
}

/*
Simulates the server broadcasting to all clients (except the original sender), with a random time delay between them.
*/
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
	eventsProvider.incrementCount()
}

/*
Takes the latest message from aggChannel and consumes it.
*/
func (s *ServerVectorClock) GetMessagesVectorClock() (MessageVectorClock, error) {
	msg := <-s.aggChannel
	return msg, nil
}

/*
Starts an instance of a ServerVectorClock.
*/
func StartServerVectorClock(clientCount int, vectorClock []int) (ServerVectorClock, error) {

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

	// Spawn a child process for each client receive channel
	// Once a message is received, it is fed to aggChannnel to be consumed later.
	for _, ch := range server.ClientsSendChannel {
		go func(c chan MessageVectorClock) {
			for {
				// setting and incrementing Vector clock here signifiies that messages are processed here.
				message := <-c
				message = server.setMaxVectorClock(message)
				server.VectorClock[0]++
				fmt.Printf("Server   %v: Received a message from client %v \n", server.VectorClock, message.clientId)
				server.aggChannel <- message
				eventsProvider.incrementCount()
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

/*
Simulates sending a message to the sever.
*/
func (c *ClientVectorClock) PingServerVectorClock() bool {
	c.VectorClock[c.id+1]++
	fmt.Printf("client %v %v: pinging server \n", c.id, c.VectorClock)

	message := MessageVectorClock{
		c.id,
		fmt.Sprintf("Client %v %v: Ping server", c.id, c.VectorClock),
		c.VectorClock,
	}

	c.sendChannel <- message

	eventsProvider.incrementCount()
	return true
}

/*
Starts an instance of a client vector clock.
*/
func StartClientVectorClock(clientId int, sendChannel chan MessageVectorClock, receiveChannel chan MessageVectorClock, vectorClock []int) {

	client := ClientVectorClock{clientId, sendChannel, receiveChannel, vectorClock}

	// spawn child process to ping server
	go func() {
		for {
			client.PingServerVectorClock()
			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
		}
	}()

	// spawn child process to listen and consume messages on channel
	go func() {
		for {
			msg := <-client.receiveChannel
			client.setMaxVectorClock(msg)
			client.VectorClock[client.id+1]++
			eventsProvider.incrementCount()
			fmt.Printf("Client %v %v: message received. \n", client.id, client.VectorClock)

		}
	}()
}

/*
Detects potential casuality violation between client and message vector clocks. Prints to output if detected.
*/
func (c *ClientVectorClock) detectCausalityViolation(message MessageVectorClock) {
	currentTimestamp := c.VectorClock[c.id+1]
	messageTimestamp := message.VectorClock[c.id+1]

	if currentTimestamp > messageTimestamp {
		text := fmt.Sprintf("POTENTIAL CAUSALITY VIOLATION AT CLIENT %v \nclient vector clock  %v\nmessage vector clock %v\n", c.id, c.VectorClock, message.VectorClock)
		fmt.Println(text)
		eventsProvider.addText(text)
	}
}

/*
Detects potential casuality violation between server and message vector clocks. Prints to output if detected.
*/
func (s *ServerVectorClock) detectCausalityViolation(message MessageVectorClock) {
	currentTimestamp := s.VectorClock[0]
	messageTimestamp := message.VectorClock[0]

	if currentTimestamp > messageTimestamp {
		text := fmt.Sprintf("POTENTIAL CAUSALITY VIOLATION AT SERVER\nserver vector clock  %v\nmessage vector clock %v\n", s.VectorClock, message.VectorClock)
		fmt.Println(text)
		eventsProvider.addText(text)
	}
}

/*
Start the simulation with the required number of clients.
*/
func Part1_3() {
	rand.Seed(0) // we keep a constant seed so that results are easier to intepret.
	wg := sync.WaitGroup{}
	wg.Add(1)

	textArray := make([]string, 0)
	eventsProvider = &EventsProvider{0, &sync.Mutex{}, &wg, textArray}

	fmt.Println("Simulating vector clock...")
	time.Sleep(time.Second * 2)

	server, err := StartServerVectorClock(NUMBER_OF_CLIENTS, make([]int, NUMBER_OF_CLIENTS+1))

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		StartClientVectorClock(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i], make([]int, NUMBER_OF_CLIENTS+1))
	}

	wg.Wait()
	eventsProvider.printCausalityViolations()
}
