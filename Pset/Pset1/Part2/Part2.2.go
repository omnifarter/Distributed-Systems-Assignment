package Pset2

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

/*
The machine will be killed after announcing its victory to the first 2 machines.
*/
func (m *Machine) startElectionAndCoordinatorFails() {
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
		if i == 2 { // we kill the machine once it notifies first 2 machines of victory.
			m.sendChannel <- Message{
				messageType: QUIT,
				senderId:    m.id,
				receiverId:  m.id,
			}
			return
		}
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
A random machine will be killed after announcing coordinator victory midway.
*/
func (m *Machine) startElectionAndRandomFails() {
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
		if i == 2 { // we kill the next machine machine once it notifies first 2 machines of victory.
			m.sendChannel <- Message{
				messageType: QUIT,
				senderId:    i,
				receiverId:  i,
			}
		} else {
			fmt.Printf("Notifying Machine %v \n", i)
			m.sendChannel <- Message{
				messageType: NOTIFY_VICTORY,
				receiverId:  i,
				senderId:    m.id,
			}
		}
	}

}

/*
We randomly start an election. this function is hardcoded to sleep for 5 seconds for simplification.
*/
func (m *Machine) randomElectionChallenge() {
	time.Sleep(time.Duration(5) * time.Second)
	go m.startElection()
}

/*
The coordinator node fails while announcing that it has won the election. We resolve this by having a machine randomly start an election.
*/
func Part2_2_a() {
	wg := sync.WaitGroup{}

	messageDistributor, machines := initialise(&wg)

	toKill := NUMBER_OF_CLIENTS - 1
	toStart := NUMBER_OF_CLIENTS - 2 // this is hardcoded to simulate fastest case

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
	go startingMachine.startElectionAndCoordinatorFails()
	randomMachine := rand.Intn(3)
	go machines[randomMachine].randomElectionChallenge() // we will start an election randomly.
	wg.Wait()
	fmt.Println("Election has ended.")
}

/*
While announcing that a coordinator has won the election, one of the machines(2) fails.
*/
func Part2_2_b() {
	wg := sync.WaitGroup{}

	messageDistributor, machines := initialise(&wg)

	toKill := NUMBER_OF_CLIENTS - 1
	toStart := NUMBER_OF_CLIENTS - 2 // this is hardcoded to simulate fastest case

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
	go startingMachine.startElectionAndRandomFails()
	randomMachine := rand.Intn(3)
	go machines[randomMachine].randomElectionChallenge() // we will start an election randomly.
	wg.Wait()
	fmt.Println("Election has ended.")
}
