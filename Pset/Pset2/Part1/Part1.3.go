package Pset2

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	REQUEST_COORDINATOR = 0
	REQUEST_APPROVED    = 1
	RELEASE_CS          = 2
)

var globalCentralMachines []*CentralMachine

type CentralMessage struct {
	messageType int
	senderId    int
	receiverId  int
}
type CentralMachine struct {
	id int

	messageChan chan CentralMessage

	//For coordinator
	queue             []CentralMessage
	coordinatorId     int
	currentLockHolder int // is -1 if not in use
	mu                *sync.Mutex
}

/*
Called as a Go routine to listen on messageChan
*/
func (m *CentralMachine) listen() {
	for {
		msg := <-m.messageChan
		switch msg.messageType {
		case REQUEST_COORDINATOR:
			m.onRequestCoordinator(msg) //
		case REQUEST_APPROVED: //
			m.onRequestApproved(msg)
		case RELEASE_CS:
			m.onReleaseCS(msg)
		}
	}
}

/*
If request is approved by coordinator, execute CS.
Once done, send RELEASE_CS message back to coordinator.
*/
func (m *CentralMachine) onRequestApproved(msg CentralMessage) {
	fmt.Printf("Machine %v - executing CS...\n", m.id)
	time.Sleep(time.Millisecond * CS_DURATION)
	globalCentralMachines[m.coordinatorId].messageChan <- CentralMessage{
		RELEASE_CS, m.id, m.coordinatorId,
	}
}

/*
Coordinator receives request to enter CS. Grants request if lock is not held by other machine, else add request to queue.
*/
func (m *CentralMachine) onRequestCoordinator(msg CentralMessage) {
	if m.currentLockHolder != -1 { // add to queue if pending CS.
		fmt.Printf("Machine %v - lock currently held, adding to queue\n", m.id)
		m.addToQueue(msg)
	} else {
		fmt.Printf("Machine %v - granting lock to %v\n", m.id, msg.senderId)
		globalCentralMachines[msg.senderId].messageChan <- CentralMessage{
			REQUEST_APPROVED, m.id, msg.senderId,
		}
		m.currentLockHolder = msg.senderId
	}
}

/*
Coordinator receives RELASE_CS message from machine. Resets lock holder, and if queue is not empty, grants it to next in queue.
*/
func (m *CentralMachine) onReleaseCS(msg CentralMessage) {
	m.currentLockHolder = -1

	fmt.Printf("Machine %v - lock released\n", m.id)
	if len(m.queue) > 0 {
		msg := m.removeFromQueue()
		fmt.Printf("Machine %v - granting lock to %v\n", m.id, msg.senderId)
		globalCentralMachines[msg.senderId].messageChan <- CentralMessage{
			REQUEST_APPROVED, m.id, msg.senderId,
		}
	} else {
		executeNextLoopCentral()
	}
}

/*
Adds the message to the priority queue.
*/
func (m *CentralMachine) addToQueue(msg CentralMessage) {
	m.mu.Lock()
	m.queue = append(m.queue, msg)
	// sort by smaller senderId.
	sort.SliceStable(m.queue, func(i, j int) bool {
		if m.queue[i].senderId < m.queue[j].senderId {
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
func (m *CentralMachine) removeFromQueue() CentralMessage {
	m.mu.Lock()
	msg := m.queue[0]
	m.queue = m.queue[1:]
	m.mu.Unlock()
	return msg
}

/*
Machine sends request to coordinator to enter CS.
*/
func (m *CentralMachine) requestForCS() {
	fmt.Printf("Machine %v - Requesting to enter CS\n", m.id)
	globalCentralMachines[m.coordinatorId].messageChan <- CentralMessage{REQUEST_COORDINATOR, m.id, m.coordinatorId}
}

/*
Initialise all machines
*/
func initialiseCentral() {
	writeFile, _ = os.Create("./Pset/Pset2/Part1/Part1.3.txt")

	globalCentralMachines = make([]*CentralMachine, NUMBER_OF_NODES)

	for i := 0; i < NUMBER_OF_NODES; i++ {
		machine := CentralMachine{
			i,
			make(chan CentralMessage, NUMBER_OF_NODES+1),
			make([]CentralMessage, 0),
			0, // machine 0 is always coordinator.
			-1,
			&sync.Mutex{},
		}
		go machine.listen()
		globalCentralMachines[i] = &machine
	}
}

/*
Function to increment and execute loop for simultaneous CS access. It keeps track of time taken to execute each loop and stores them in a txt file.
*/
func executeNextLoopCentral() {
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
			globalCentralMachines[i].requestForCS()
		}
	}
}

func Part_1_3() {
	initialiseCentral()
	wg.Add(1)
	executeNextLoopCentral()
	wg.Wait()
}
