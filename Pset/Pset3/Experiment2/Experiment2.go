package pset3

import (
	FTIvy "DistributedSystemsAssignment/Pset/Pset3/FTIvy"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

func Experiment2a() {
	FTIvy.Experiment2a()

	timings, err := ioutil.ReadFile("./Pset/Pset3/Experiment2/experiment_2a_timings.txt")

	if err != nil {
		log.Fatal(err)
	}

	splitTimings := strings.Split(string(timings), "\n")

	fmt.Printf("Time taken to finish write with primary replica fault: %v\n", splitTimings[0])
	fmt.Printf("Time taken to finish write without faults            : %v\n", splitTimings[1])
}

func Experiment2b() {
	FTIvy.Experiment2b()
	timings, err := ioutil.ReadFile("./Pset/Pset3/Experiment2/experiment_2b_timings.txt")

	if err != nil {
		log.Fatal(err)
	}

	splitTimings := strings.Split(string(timings), "\n")

	fmt.Printf("Time taken to finish write with primary replica fault: %v\n", splitTimings[0])
	fmt.Printf("Time taken to finish write without faults            : %v\n", splitTimings[1])

}
