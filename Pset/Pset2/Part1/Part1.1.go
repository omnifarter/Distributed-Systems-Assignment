package Pset2

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	NUMBER_OF_NODES = 11
	CS_DURATION     = 1000 //milliseconds

	//message types
	REQUEST = 0
	REPLY   = 1
)

var writeFile *os.File
var wg = sync.WaitGroup{}
var lastMachine = 0
var startTime time.Time
var loopReplies = make([]bool, 0)
var globalMachines []*Machine

type Message struct {
	timestamp   int
	duration    int
	messageType int
	clock       int
	senderId    int
	receiverId  int
}

type Machine struct {
	id int

	messageChan chan Message
	queue       []Message
	clock       int
	replies     map[int]map[int]bool // -> this is a map of requests to a map of machine ids.

	mu *sync.Mutex
}

/*
Called as a Go routine to listen on messageChan
*/
func (m *Machine) listen() {
	for {
		msg := <-m.messageChan
		m.updateLogicalClock(msg)

		switch msg.messageType {
		case REQUEST:
			m.onRequest(msg)
		case REPLY:
			m.onReply(msg)
		}
	}
}

/*
Called as a Go routine to listen for replies. This is only called when machine wants to enter CS.
*/
func (m *Machine) waitForReplies(msg Message) {
	for {

		var receivedAllReplies = true
		for i := 0; i < NUMBER_OF_NODES; i++ {
			m.mu.Lock()
			_, ok := m.replies[msg.timestamp][i]
			m.mu.Unlock()
			if !ok {
				receivedAllReplies = false
				break
			}
		}
		if receivedAllReplies {
			fmt.Printf("Machine %v - received all replies, executing request %v \n", m.id, msg.timestamp)
			m.executeCS(msg)
			return
		}
	}

}

/*
When the machine receives a REQUEST message. Either replies or adds message to queue.
*/
func (m *Machine) onRequest(msg Message) {
	//Check if any message in the queue has an earlier timestamp than incoming msg. if there is, msg will be added to queue.
	waiting := false

	if len(m.queue) > 0 {
		m.mu.Lock()
		for _, v := range m.queue {
			if v.timestamp < msg.timestamp || v.timestamp == msg.timestamp && v.senderId < msg.senderId {
				waiting = true
				break
			}
		}
		m.mu.Unlock()
	}

	if waiting {
		fmt.Printf("Machine %v - Received request from %v but CS is pending. Added to queue.\n", m.id, msg.senderId)
		m.addToQueue(msg)
	} else {
		globalMachines[msg.senderId].messageChan <- Message{
			msg.timestamp, msg.duration, REPLY, m.clock, m.id, msg.senderId,
		}
	}
}

/*
When the machine receives a REPLY message.
*/
func (m *Machine) onReply(msg Message) {
	m.mu.Lock()
	m.replies[msg.timestamp][msg.senderId] = true
	m.mu.Unlock()
}

/*
Logical clock helper function.
*/
func (m *Machine) updateLogicalClock(msg Message) {
	if msg.clock > m.clock {
		m.clock = msg.clock
	}
	m.clock += 1
}

/*
Adds the message to the priority queue.
*/
func (m *Machine) addToQueue(msg Message) {
	m.mu.Lock()
	m.queue = append(m.queue, msg)
	// Sort queue based on timestamp. If timestamp is equal, sort by smaller senderId.
	sort.SliceStable(m.queue, func(i, j int) bool {
		if m.queue[i].timestamp < m.queue[j].timestamp {
			return true
		} else if m.queue[i].timestamp == m.queue[j].timestamp && m.queue[i].senderId < m.queue[j].senderId {
			return true
		} else {
			return false
		}
	})
	m.mu.Unlock()

}

/*
Removes and returns the message at the head of the queue.
*/
func (m *Machine) removeFromQueue() Message {
	m.mu.Lock()
	msg := m.queue[0]
	m.queue = m.queue[1:]
	m.mu.Unlock()
	return msg
}

/*
Executes CS and broadcasts release message once done. if this is the last machine to execute CS, starts the next loop.
*/
func (m *Machine) executeCS(msg Message) {
	m.removeFromQueue()
	time.Sleep(time.Duration(msg.duration) * time.Millisecond) //We sleep for 1 second when executing CS
	m.broadcastRelease()
	fmt.Printf("Machine %v - finished request %v \n", m.id, msg.timestamp)
	loopReplies = append(loopReplies, true)
	if len(loopReplies) == lastMachine { //if this is the last machine of the loop, start a new loop
		executeNextLoop()
	}
}

/*
Broadcasts the release message.
*/
func (m *Machine) broadcastRelease() {
	m.clock += 1
	for _, msg := range m.queue {
		globalMachines[msg.senderId].messageChan <- Message{msg.timestamp, msg.duration, REPLY, m.clock, m.id, msg.senderId}
		m.removeFromQueue()
	}
}

/*
Requests for CS. Sends REQUEST message to all other nodes, and spins up go routine to wait for replies.
*/
func (m *Machine) requestForCS() {
	m.clock += 1
	timestamp := m.clock
	fmt.Printf("Machine %v - Requesting to enter CS, timestamped at %v\n", m.id, m.clock)
	msg := Message{timestamp, CS_DURATION, REQUEST, m.clock, m.id, m.id}
	m.replies[msg.timestamp] = map[int]bool{}
	m.replies[msg.timestamp][m.id] = true
	m.addToQueue(msg)
	for i := 0; i < NUMBER_OF_NODES; i++ {
		if m.id == i {
			continue
		}
		globalMachines[i].messageChan <- Message{timestamp, CS_DURATION, REQUEST, m.clock, m.id, i}
	}
	go m.waitForReplies(msg)

}

/*
Initialise all machines
*/
func initialise() {
	writeFile, _ = os.Create("./Pset/Pset2/Part1/Part1.1.txt")

	globalMachines = make([]*Machine, NUMBER_OF_NODES)

	for i := 0; i < NUMBER_OF_NODES; i++ {
		machine := Machine{
			i,
			make(chan Message, NUMBER_OF_NODES+1),
			make([]Message, 0),
			0,
			map[int]map[int]bool{},
			&sync.Mutex{},
		}
		go machine.listen()
		globalMachines[i] = &machine
	}
}

/*
Function to increment and execute loop for simultaneous CS access. It keeps track of time taken to execute each loop and stores them in a txt file.
*/
func executeNextLoop() {
	if lastMachine > 0 {
		diff := time.Since(startTime)
		writeFile.WriteString(diff.String() + "\n")
	}

	if lastMachine == NUMBER_OF_NODES {
		writeFile.Close()
		wg.Done()
	} else {
		fmt.Println("Starting next loop...")
		startTime = time.Now()
		lastMachine += 1
		loopReplies = make([]bool, 0)
		for i := 0; i < lastMachine; i++ {
			globalMachines[i].requestForCS()
		}
	}
}

func Part_1_1() {
	initialise()
	wg.Add(1)
	executeNextLoop()
	wg.Wait()
}
