package Pset2

import (
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	REQUEST_FOR_VOTE = 0
	VOTE_FOR         = 1
	RELEASE_VOTE     = 2
	RESCIND_VOTE     = 4
)

var globalVotingMachines []*VotingMachine

type VotingMachine struct {
	id int

	messageChan chan Message
	queue       []Message
	clock       int
	votes       []int // number of votes

	mu *sync.Mutex

	// voting protocol
	votedFor     int // if -1, means that machine has not voted.
	muvotedFor   *sync.Mutex
	votedMessage Message
	executingCS  bool
}

func (m *VotingMachine) listen() {
	for {
		msg := <-m.messageChan
		m.updateLogicalClock(msg)
		switch msg.messageType {
		case REQUEST_FOR_VOTE:
			m.onRequest(msg)
		case VOTE_FOR:
			m.onVote(msg)
		case RELEASE_VOTE:
			m.onRelease()
		case RESCIND_VOTE:
			m.onRescind(msg)
		}
	}
}

func (m *VotingMachine) onRescind(msg Message) {
	if !m.executingCS {
		for i, votee := range m.votes { // remove vote from votes array
			if votee == msg.senderId {
				m.votes = append(m.votes[:i], m.votes[i+1:]...)
				globalVotingMachines[i].messageChan <- Message{m.clock, CS_DURATION, RELEASE_VOTE, m.clock, m.id, i} // releases vote back to the machines that voted.
			}
		}
	}
}
func (m *VotingMachine) waitForReplies(msg Message) {
	for {
		if len(m.votes) > int(math.Ceil(NUMBER_OF_NODES/2)) {
			fmt.Printf("Machine %v - received majority of votes, executing request %v %v\n", m.id, msg.timestamp, m.votes)
			m.executeCS(msg)
			return
		}
	}

}

func (m *VotingMachine) onRequest(msg Message) {
	m.muvotedFor.Lock()
	if m.votedFor != -1 {
		if (m.votedMessage.timestamp > msg.timestamp) || (m.votedMessage.timestamp == msg.timestamp && m.votedMessage.senderId > msg.senderId) { // we try to rescind the vote
			fmt.Printf("Machine %v - Received request from %v but CS is pending. Will try to rescind vote.\n", m.id, msg.senderId)
			globalVotingMachines[m.votedFor].messageChan <- Message{
				m.votedMessage.timestamp, m.votedMessage.duration, RESCIND_VOTE, m.clock, m.id, m.votedFor,
			}
		}
		m.addToQueue(msg)
	} else {
		m.votedFor = msg.senderId
		m.votedMessage = msg
		globalVotingMachines[msg.senderId].messageChan <- Message{
			msg.timestamp, msg.duration, VOTE_FOR, m.clock, m.id, msg.senderId,
		}
	}
	m.muvotedFor.Unlock()
}

func (m *VotingMachine) onVote(msg Message) {
	m.mu.Lock()
	if m.executingCS {
		fmt.Println("THIS is already running CS! returning vote...")
		globalVotingMachines[msg.senderId].messageChan <- Message{m.clock, CS_DURATION, RELEASE_VOTE, m.clock, m.id, msg.senderId} // releases vote back to the machines that voted.
	} else {
		m.votes = append(m.votes, msg.senderId)
	}
	m.mu.Unlock()
}

func (m *VotingMachine) updateLogicalClock(msg Message) {
	if msg.clock > m.clock {
		m.clock = msg.clock
	}
	m.clock += 1
}

func (m *VotingMachine) addToQueue(msg Message) {
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

func (m *VotingMachine) removeFromQueue() Message {
	m.mu.Lock()
	msg := m.queue[0]
	m.queue = m.queue[1:]
	m.mu.Unlock()
	return msg
}

func (m *VotingMachine) executeCS(msg Message) {
	m.executingCS = true
	time.Sleep(time.Duration(msg.duration) * time.Millisecond) //We sleep for 1 second when executing CS
	m.executingCS = false
	m.broadcastRelease()
	fmt.Printf("Machine %v - finished request %v \n", m.id, msg.timestamp)
	loopReplies = append(loopReplies, true)
	if len(loopReplies) == lastMachine { //if this is the last machine of the loop, start a new loop
		executeNextLoopVoting()
	}
}

func (m *VotingMachine) broadcastRelease() {
	m.clock += 1
	for _, mid := range m.votes {
		globalVotingMachines[mid].messageChan <- Message{m.clock, CS_DURATION, RELEASE_VOTE, m.clock, m.id, mid} // releases vote back to the machines that voted.
	}
	m.votes = make([]int, 0)
	m.onRelease()
}

func (m *VotingMachine) onRelease() {
	m.muvotedFor.Lock()
	m.votedFor = -1
	m.votedMessage = Message{}
	if len(m.queue) > 0 {
		msg := m.removeFromQueue()
		globalVotingMachines[msg.senderId].messageChan <- Message{msg.timestamp, msg.duration, VOTE_FOR, m.clock, m.id, msg.senderId} // vote for machine at head of the queue.
		m.votedFor = msg.senderId
		m.votedMessage = msg
	}
	m.muvotedFor.Unlock()
}
func (m *VotingMachine) requestForCS() {
	m.clock += 1
	timestamp := m.clock
	fmt.Printf("Machine %v - Requesting to enter CS, timestamped at %v\n", m.id, m.clock)
	msg := Message{timestamp, CS_DURATION, REQUEST_FOR_VOTE, m.clock, m.id, m.id}
	m.muvotedFor.Lock()
	if m.votedFor == -1 {
		m.votedFor = m.id
		m.onVote(msg)
	} else {
		m.addToQueue(msg)
	}
	m.muvotedFor.Unlock()
	for i := 0; i < NUMBER_OF_NODES; i++ {
		if m.id == i {
			continue
		}
		globalVotingMachines[i].messageChan <- Message{timestamp, CS_DURATION, REQUEST_FOR_VOTE, m.clock, m.id, i}
	}
	go m.waitForReplies(msg)

}

func initialiseVoting() {
	writeFile, _ = os.Create("./Pset/Pset2/Part1/Part1.2.txt")

	globalVotingMachines = make([]*VotingMachine, NUMBER_OF_NODES)

	for i := 0; i < NUMBER_OF_NODES; i++ {
		machine := VotingMachine{
			i,
			make(chan Message, NUMBER_OF_NODES+1),
			make([]Message, 0),
			0,
			make([]int, 0),
			&sync.Mutex{},
			-1,
			&sync.Mutex{},
			Message{},
			false,
		}
		go machine.listen()
		globalVotingMachines[i] = &machine
	}
}

// Function to increment and execute loop for simultaneous CS access. It keeps track of time taken to execute each loop and stores them in a txt file.
func executeNextLoopVoting() {
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
			globalVotingMachines[i].requestForCS()
		}
	}
}
func Part_1_2() {
	initialiseVoting()
	wg.Add(1)
	executeNextLoopVoting()
	wg.Wait()
}
