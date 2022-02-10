package main

import (
	"fmt"

	pset_1 "DistributedSystemsAssignment/Pset/Pset1"

	"github.com/manifoldco/promptui"
)

func Pset1() {
	// select Problem Set assignment parts
	prompt := promptui.Select{
		Label: "Select which part to simulate.",
		Items: []string{
			"Part 1.1 - Client server architecture",
			"Part 1.2 - Lampart logical clock",
		},
	}
	_, result, err := prompt.Run()

	if err != nil {
		fmt.Println("Exiting...")
		return
	}

	switch result {
	case "Part 1.1 - Client server architecture":
		pset_1.Part1_1()
	case "Part 1.2 - Lampart logical clock":
		pset_1.Part1_2()

	}
}
