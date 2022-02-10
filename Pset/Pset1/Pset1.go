package Pset1

import (
	"fmt"
	"sync"
	"time"
)

/*
	Start the simulation with the required number of clients.
	WaitGroup is used here to keep the simulation running indefinitely.
*/
func Part1() {
	NUMBER_OF_CLIENTS := 4
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating client server architecture...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)

	server, err := StartServer(NUMBER_OF_CLIENTS)

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.clientsReceiveChannel); i++ {
		StartClient(i, server.clientsSendChannel[i], server.clientsReceiveChannel[i])
	}

	wg.Wait()
}

func Part2() {

}
