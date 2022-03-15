package Pset2

import (
	"fmt"
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

func (m *Machine) onRequest(msg Message) {

	pending := false
	//Check if any message in the queue has an earlier timestamp than incoming msg. if there is, msg will be added to queue.
	for _, queueMsg := range m.queue {
		if queueMsg.timestamp < msg.timestamp && queueMsg.senderId != msg.senderId {
			pending = true
			break
		}
	}
	if pending {
		fmt.Printf("Machine %v - Received request from %v but CS is pending. Added to queue.\n", m.id, msg.senderId)
		m.addToQueue(msg)
	} else {
		globalMachines[msg.senderId].messageChan <- Message{
			msg.timestamp, msg.duration, REPLY, m.clock, m.id, msg.senderId,
		}
	}
}

func (m *Machine) onReply(msg Message) {
	m.mu.Lock()
	m.replies[msg.timestamp][msg.senderId] = true
	m.mu.Unlock()
}

func (m *Machine) updateLogicalClock(msg Message) {
	if msg.clock > m.clock {
		m.clock = msg.clock
	}
	m.clock += 1
}

func (m *Machine) addToQueue(msg Message) {
	m.queue = append(m.queue, msg)
	// Sort queue based on timestamp. If timestamp is equal, sort by senderId.
	sort.SliceStable(m.queue, func(i, j int) bool {
		if m.queue[i].timestamp < m.queue[j].timestamp {
			return true
		} else if m.queue[i].timestamp == m.queue[j].timestamp && m.queue[i].senderId < m.queue[j].senderId {
			return true
		} else {
			return false
		}
	})
}

func (m *Machine) removeFromQueue() Message {
	msg := m.queue[0]
	m.queue = m.queue[1:]
	return msg
}

func (m *Machine) executeCS(msg Message) {
	m.removeFromQueue()
	time.Sleep(time.Duration(msg.duration) * time.Millisecond)
	m.broadcastRelease()
	fmt.Printf("Machine %v - finished request %v \n", m.id, msg.timestamp)
}

func (m *Machine) broadcastRelease() {
	m.clock += 1
	for _, msg := range m.queue {
		globalMachines[msg.senderId].messageChan <- Message{msg.timestamp, msg.duration, REPLY, m.clock, m.id, msg.senderId}
	}
}

func (m *Machine) requestForCS() {
	m.clock += 1
	timestamp := m.clock
	fmt.Printf("Machine %v - Requesting to enter CS, timestamped at %v\n", m.id, m.clock)
	msg := Message{timestamp, CS_DURATION, REQUEST, m.clock, m.id, m.id}
	m.replies[msg.timestamp] = map[int]bool{}
	m.replies[msg.timestamp][m.id] = true
	m.queue = append(m.queue, msg)
	for i := 0; i < NUMBER_OF_NODES; i++ {
		if m.id == i {
			continue
		}
		globalMachines[i].messageChan <- Message{timestamp, CS_DURATION, REQUEST, m.clock, m.id, i}
	}
	go m.waitForReplies(msg)

}
func initialise() {
	globalMachines = make([]*Machine, NUMBER_OF_NODES)

	for i := 0; i < NUMBER_OF_NODES; i++ {
		machine := Machine{
			i,
			make(chan Message),
			make([]Message, 0),
			0,
			map[int]map[int]bool{},
			&sync.Mutex{},
		}
		go machine.listen()
		globalMachines[i] = &machine
	}
}

func Part_1_1() {
	initialise()
	wg := sync.WaitGroup{}
	wg.Add(1)
	globalMachines[0].requestForCS()
	globalMachines[1].requestForCS()
	globalMachines[2].requestForCS()
	globalMachines[3].requestForCS()
	globalMachines[4].requestForCS()
	globalMachines[5].requestForCS()
	wg.Wait()
}
