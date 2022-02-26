package Pset1

import (
	part1 "DistributedSystemsAssignment/Pset/Pset1/Part1"
	part2 "DistributedSystemsAssignment/Pset/Pset1/Part2"
	"fmt"
	"sync"
	"time"
)

/*
Start the simulation with the required number of clients.
WaitGroup is used here to keep the simulation running indefinitely.
*/
func Part1_1() {
	NUMBER_OF_CLIENTS := 4
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating client server architecture...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)

	server, err := part1.StartServer(NUMBER_OF_CLIENTS)

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		part1.StartClient(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i])
	}

	wg.Wait()
}

/*
Start the simulation with the required number of clients.
*/
func Part1_2() {
	NUMBER_OF_CLIENTS := 4
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating lampart's logical clock...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)

	server, err := part1.StartServerLogicalClock(NUMBER_OF_CLIENTS)

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		part1.StartClientLogicalClock(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i])
	}

	wg.Wait()

}

/*
Start the simulation with the required number of clients.
*/
func Part1_3() {
	NUMBER_OF_CLIENTS := 4
	wg := sync.WaitGroup{}
	wg.Add(1)

	fmt.Println("Simulating vector clock...")
	fmt.Println("Press Ctrl + c to stop program execution.")

	time.Sleep(time.Second * 2)
	fmt.Println("vectorClock instantiating.")

	//populate vector clock with all 0s
	vectorClock := make([]int, NUMBER_OF_CLIENTS+1)
	fmt.Println("vectorClock instantiated.")

	server, err := part1.StartServerVectorClock(NUMBER_OF_CLIENTS, vectorClock)

	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < len(server.ClientsReceiveChannel); i++ {
		part1.StartClientVectorClock(i, server.ClientsSendChannel[i], server.ClientsReceiveChannel[i], vectorClock)
	}

	wg.Wait()

}

func Part2_1(scenario string) {
	if scenario == "BEST" {
		part2.Part2_1_BEST()
	} else {
		// part2.Demo()
		fmt.Println("WORST CASE")
	}
}
