package Pset2

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	MAX_RTT           = 1000 // stored as milliseconds
	NUMBER_OF_CLIENTS = 5

	// types of messages
	SELF_ELECTION  = 0
	REPLY          = 1
	NOTIFY_VICTORY = 2
	QUIT           = 3
	END            = 4
)

/*
We use the GlobalContainer to hold an alive count with a mutex lock.
aliveCount is used to ensure that we can gracefully exit the program when multiple processes are calling an election.
*/
type GlobalContainer struct {
	mu         sync.Mutex
	aliveCount int
}

var globalContainer *GlobalContainer

/*
decrementWg is used instead of calling wg.Done() directly,
because we want to ensure that wg.Done() is called no more than NUMBER_OF_CLIENTS times by the various go routines.
this ensures thread safety.
*/
func decrementWg(wg *sync.WaitGroup) error {
	globalContainer.mu.Lock()
	if globalContainer.aliveCount == 0 {
		return errors.New("no more machines are alive")
	}
	wg.Done()
	globalContainer.aliveCount -= 1
	globalContainer.mu.Unlock()
	return nil
}

type Machine struct {
	id             int
	coordinatorId  int
	sendChannel    chan Message
	receiveChannel chan Message

	wg *sync.WaitGroup
}

/*
MessageDistributor is used to help facilitate sending of messages during broadcasting.
It is not meant to represent a central messaging system, and is to make coding easier.
*/
type MessageDistributor struct {
	receiveChannels []chan Message
	sendChannels    []chan Message
	aggChannel      chan Message
	channelStatus   map[int]bool
}

type Message struct {
	messageType int
	receiverId  int
	senderId    int
}

func releaseWaitGroup(wg *sync.WaitGroup) {
	for i := 0; i < NUMBER_OF_CLIENTS; i++ {
		err := decrementWg(wg)
		if err != nil {
			return
		}
	}
}

/*
Machine starts the election by trying to self elect.
Note it only sends to machines with a greater ID.
*/
func (m *Machine) startElection() {
	fmt.Printf("Machine %v is starting an election\n", m.id)

	m.coordinatorId = m.id

	m.coordinatorId = m.id
	for i := m.id + 1; i < NUMBER_OF_CLIENTS; i++ {

		if m.coordinatorId != m.id { // if reply message has been consumed, exit.
			return
		}

		fmt.Printf("Machine %v notifies Machine %v of the new coordinator\n", m.id, i)
		m.sendChannel <- Message{
			messageType: SELF_ELECTION,
			receiverId:  i,
			senderId:    m.id,
		}
		time.Sleep(MAX_RTT * time.Millisecond) // we wait the max RTT

	}
	fmt.Printf("Machine %v has won the coordinator role!\n", m.id)

	for i := 0; i < NUMBER_OF_CLIENTS; i++ { // election has been won, notify victory
		fmt.Printf("Notifying Machine %v \n", i)
		m.sendChannel <- Message{
			messageType: NOTIFY_VICTORY,
			receiverId:  i,
			senderId:    m.id,
		}
	}
	time.Sleep(MAX_RTT * time.Millisecond) // we wait the max RTT so that all messages are properly propogated

	releaseWaitGroup(m.wg) // gracefully exits
}

/*
Child process to consume messages from machine's receive channel.
*/
func (m *Machine) getMessages() {

	for {

		switch msg := <-m.receiveChannel; msg.messageType {
		case QUIT:
			fmt.Printf("Machine %v is killed.\n", m.id)
			return

		case REPLY: // self election has failed
			fmt.Printf("Machine %v has lost the election to machine %v\n", m.id, msg.senderId)

			m.coordinatorId = msg.senderId

		case NOTIFY_VICTORY:
			fmt.Printf("Machine %v acknowledges that Machine %v is the new coordinator.\n", m.id, msg.senderId)

			m.coordinatorId = msg.senderId

			// return

		case SELF_ELECTION:
			if m.id > msg.senderId { // reply and elect itself
				fmt.Printf("Machine %v denies Machine %v is the new coordinator and starts a new election.\n", m.id, msg.senderId)
				m.sendChannel <- Message{
					messageType: REPLY,
					receiverId:  msg.senderId,
					senderId:    m.id,
				}
				go m.startElection()
			} else { // assign the new coordinator

				m.coordinatorId = msg.senderId

			}
		}
	}

}

/*
Message distributor checks aggChannel and routes
*/
func (d *MessageDistributor) routeMessages() {
	for {
		msg := <-d.aggChannel
		if d.channelStatus[msg.receiverId] {
			if msg.messageType == QUIT {
				d.channelStatus[msg.senderId] = false
			}
			d.sendChannels[msg.receiverId] <- msg
		}
	}
}

func initialise(wg *sync.WaitGroup) (MessageDistributor, []*Machine) {
	fmt.Println("Initialising...")
	var receiveChannels []chan Message
	var sendChannels []chan Message
	var machines []*Machine
	status := make(map[int]bool)

	globalContainer = &GlobalContainer{sync.Mutex{}, NUMBER_OF_CLIENTS}
	for i := 0; i < NUMBER_OF_CLIENTS; i++ {
		wg.Add(1)
		receiveChannels = append(receiveChannels, make(chan Message))
		sendChannels = append(sendChannels, make(chan Message))
		mach := Machine{i, NUMBER_OF_CLIENTS - 1, receiveChannels[i], sendChannels[i], wg}
		go mach.getMessages()
		machines = append(machines, &mach)
		status[i] = true
	}

	messageDistributor := MessageDistributor{receiveChannels, sendChannels, make(chan Message), status}

	go messageDistributor.routeMessages()
	/*
		spawn a child process for each receive channel
		once a message is received, it is fed to aggChannnel for broadcasting
	*/
	for _, ch := range messageDistributor.receiveChannels {
		go func(c chan Message) {
			for {
				msg := <-c
				if messageDistributor.channelStatus[msg.receiverId] {
					messageDistributor.aggChannel <- msg
				} else {
					fmt.Printf("Message won't go through, Machine %v is down. \n", msg.receiverId)
				}
			}

		}(ch)
	}
	fmt.Println("Initialised.")
	return messageDistributor, machines
}

/*
We kill the coordinator node, which will be the highest machine id.
For the best case, the next highest machine id discovers that coordinator is down,
and kick starts the election.
*/
func Part2_1_BEST() {
	wg := sync.WaitGroup{}

	messageDistributor, machines := initialise(&wg)

	toKill := NUMBER_OF_CLIENTS - 1
	toStart := NUMBER_OF_CLIENTS - 2 // this is hardcoded to simulate best case

	fmt.Printf("Killing machine %v... \n", toKill)
	messageDistributor.receiveChannels[toKill] <- Message{
		messageType: QUIT,
		senderId:    toKill,
		receiverId:  toKill,
	}
	time.Sleep(1 * time.Second)

	fmt.Printf("kick starting election from machine %v... \n", toStart)
	go machines[toStart].startElection()
	wg.Wait()
	fmt.Println("Election has ended.")
}

/*
We kill the coordinator node, which will be the highest machine id.
For the worst case, the earliest machine id discovers that coordinator is down,
and kick starts the election.
*/
func Part2_1_WORST() {
	wg := sync.WaitGroup{}

	messageDistributor, machines := initialise(&wg)

	toKill := NUMBER_OF_CLIENTS - 1
	toStart := 0 // this is hardcoded to simulate worst case

	fmt.Printf("Killing machine %v... \n", toKill)
	messageDistributor.receiveChannels[toKill] <- Message{
		messageType: QUIT,
		senderId:    toKill,
		receiverId:  toKill,
	}
	time.Sleep(1 * time.Second)
	messageDistributor.channelStatus[toKill] = false

	fmt.Printf("kick starting election from machine %v... \n", toStart)
	startingMachine := machines[toStart]
	go startingMachine.startElection()
	wg.Wait()
	fmt.Println("Election has ended.")

}
