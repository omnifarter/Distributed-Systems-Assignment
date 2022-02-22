package Pset2

import (
	"fmt"
	"time"
)

const (
	MAX_RTT           = 1000 // stored as milliseconds
	NUMBER_OF_CLIENTS = 5

	// types of messages
	RANDOM_MESSAGE = 0
	SELF_ELECTION  = 1
	REPLY          = 2
	NOTIFY_VICTORY = 3
	QUIT           = 4
)

type Machine struct {
	id             int
	coordinatorId  int
	sendChannel    chan Message
	receiveChannel chan Message
}

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

/*
Machine starts the election by trying to self elect.
Note it only sends to machines with a greater ID.
*/
func (m *Machine) startElection() {
	fmt.Printf("Machine %v is starting an election\n", m.id)
	m.coordinatorId = m.id
	for i := m.id + 1; i < NUMBER_OF_CLIENTS; i++ {
		fmt.Printf("Machine %v notifies Machine %v of the new coordinator\n", m.id, i)
		m.sendChannel <- Message{
			messageType: SELF_ELECTION,
			receiverId:  i,
			senderId:    m.id,
		}
		time.Sleep(MAX_RTT * time.Second) // we wait the max RTT
		if m.coordinatorId != m.id {      // if reply message has been consumed, exit.
			return
		}
	}
	fmt.Printf("Machine %v has won the coordinator role!\n", m.id)

	for i := m.id + 1; i < NUMBER_OF_CLIENTS; i++ { // election has been won, notify victory
		m.sendChannel <- Message{
			messageType: NOTIFY_VICTORY,
			receiverId:  i,
			senderId:    m.id,
		}
	}
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
			m.coordinatorId = msg.senderId

		case NOTIFY_VICTORY:
			fmt.Printf("Machine %v acknowledges that Machine %v is the new coordinator.\n", m.id, msg.senderId)
			m.coordinatorId = msg.senderId

		case RANDOM_MESSAGE: // send back a random message to acknowledge
			m.sendChannel <- Message{
				messageType: RANDOM_MESSAGE,
				receiverId:  msg.senderId,
				senderId:    m.id,
			}

		case SELF_ELECTION:
			if m.id > msg.senderId { // reply and elect itself
				fmt.Printf("Machine %v denies Machine %v is the new coordinator and starts a new election.\n", m.id, msg.senderId)
				m.sendChannel <- Message{
					messageType: REPLY,
					receiverId:  msg.senderId,
					senderId:    m.id,
				}
				m.startElection()
			} else { // assign the new coordinator
				m.coordinatorId = msg.senderId
			}
		}
	}
}

/*
Message distributor checks aggChannel and routes
*/
func (d MessageDistributor) routeMessages() {
	for {
		msg := <-d.aggChannel
		if d.channelStatus[msg.receiverId] {
			d.sendChannels[msg.receiverId] <- msg
		} else {
			fmt.Println("THIS SHOULD BE IGNORED")
		}
	}
}

func initialise() (MessageDistributor, []Machine) {
	fmt.Println("Initialising...")
	var receiveChannels []chan Message
	var sendChannels []chan Message
	var machines []Machine
	status := make(map[int]bool)
	for i := 0; i < NUMBER_OF_CLIENTS; i++ {
		receiveChannels = append(receiveChannels, make(chan Message))
		sendChannels = append(sendChannels, make(chan Message))
		mach := Machine{i, NUMBER_OF_CLIENTS - 1, receiveChannels[i], sendChannels[i]}
		go mach.getMessages()
		machines = append(machines, mach)
		status[i] = true
	}

	messageDistributor := MessageDistributor{sendChannels, receiveChannels, make(chan Message), status}

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
					fmt.Println("DISCARDING MESSAGE")
					<- 
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
	messageDistributor, machines := initialise()

	toKill := NUMBER_OF_CLIENTS - 1
	toStart := NUMBER_OF_CLIENTS - 2 // this is hardcoded to simulate best case

	fmt.Printf("Killing machine %v... \n", toKill)
	messageDistributor.receiveChannels[toKill] <- Message{
		messageType: QUIT,
		senderId:    toKill,
		receiverId:  toKill,
	}
	messageDistributor.channelStatus[toKill] = false
	time.Sleep(1 * time.Second)

	fmt.Printf("kick starting election from machine %v... \n", toStart)
	machines[toStart].startElection()

}

func Part2_1_WORST() {

}
