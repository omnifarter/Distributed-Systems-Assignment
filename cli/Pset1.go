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
		Items: []string{"Part 1 - Client server architecture"},
	}
	_, result, err := prompt.Run()

	if err != nil {
		fmt.Println("Exiting...")
		return
	}

	switch result {
	case "Part 1 - Client server architecture":
		pset_1.Part1()
	}
}
