package pset3

import (
	FTIvy "DistributedSystemsAssignment/Pset/Pset3/FTIvy"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

func Experiment3() {
	FTIvy.Experiment3()

	timings, err := ioutil.ReadFile("./Pset/Pset3/Experiment3/experiment_3_timings.txt")

	if err != nil {
		log.Fatal(err)
	}

	splitTimings := strings.Split(string(timings), "\n")

	fmt.Printf("Time taken to finish write after 1st failure: %v\n", splitTimings[0])
	fmt.Printf("Time taken to finish write after 2nd failure: %v\n", splitTimings[1])

}
