package Pset1

import (
	"fmt"
	"sync"
	"time"
)

/*
The coordinator node fails.
All other machines (0 - 3) kickstart the election simultaneously.
*/
func Part2_3() {
	wg := sync.WaitGroup{}

	messageDistributor, machines := initialise(&wg)

	toKill := NUMBER_OF_CLIENTS - 1

	fmt.Printf("Killing machine %v... \n", toKill)
	messageDistributor.receiveChannels[toKill] <- Message{
		messageType: QUIT,
		senderId:    toKill,
		receiverId:  toKill,
	}
	time.Sleep(1 * time.Second)

	for i := 0; i < toKill; i++ {
		fmt.Printf("kick starting election from machine %v... \n", i)
		go machines[i].startElection()
	}
	wg.Wait()
	fmt.Println("Election has ended.")
}
