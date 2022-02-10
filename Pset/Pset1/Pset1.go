package Pset1

import (
	"fmt"
	"sync"
)

func Part1() {
	NUMBER_OF_CLIENTS := 4

	wg := sync.WaitGroup{}
	wg.Add(1)
	fmt.Println("Simulating client server architecture...")

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
