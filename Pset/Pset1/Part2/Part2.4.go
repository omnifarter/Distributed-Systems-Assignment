package Pset2

import (
	"fmt"
	"sync"
	"time"
)

/*
This function adds the machine to the MessageDistributor.
This is to play out the scenario where the new machine
is aware of all other machines in the network.
*/
func (d *MessageDistributor) addMachine(m Machine) {
	d.receiveChannels = append(d.receiveChannels, m.sendChannel)
	d.sendChannels = append(d.sendChannels, m.receiveChannel)
	d.channelStatus[m.id] = true
	go func(ch chan Message) {
		for {
			msg := <-ch
			if d.channelStatus[msg.receiverId] {
				d.aggChannel <- msg
			} else {
				fmt.Printf("Message won't go through, Machine %v is down. \n", msg.receiverId)
			}
		}
	}(d.receiveChannels[len(d.receiveChannels)-1])
}

/*
A fresh node joins the network.
We can make the other nodes aware of this new node, by making it start an election.
*/
func Part2_4() {
	wg := sync.WaitGroup{}

	messageDistributor, _ := initialise(&wg)

	machReceiveChannel := make(chan Message)
	machSendChannel := make(chan Message)
	mach := Machine{NUMBER_OF_CLIENTS, NUMBER_OF_CLIENTS - 1, machReceiveChannel, machSendChannel, &wg}
	fmt.Printf("Adding a new machine %v\n", mach.id)
	go mach.getMessages()

	messageDistributor.addMachine(mach)
	time.Sleep(1 * time.Second)

	go mach.startElection()
	wg.Wait()
	fmt.Println("Election has ended.")
}
