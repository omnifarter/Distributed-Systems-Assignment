package cli

import (
	"fmt"

	pset_2 "DistributedSystemsAssignment/Pset/Pset2"

	"github.com/manifoldco/promptui"
)

func Pset2() {
	// select Problem Set assignment parts
	prompt := promptui.Select{
		Label: "Select which part to simulate.",
		Items: []string{
			"Part 1.1 - Lamport's shared priority queue",
			"Part 1.2 - Voting protocol",
			"Part 1.3 - Centralised server protocol",
			"Part 2   - Performance comparison",
		},
	}
	_, result, err := prompt.Run()

	if err != nil {
		fmt.Println("Exiting...")
		return
	}

	switch result {
	case "Part 1.1 - Lamport's shared priority queue":
		pset_2.Part1_1()
	case "Part 1.2 - Voting protocol":
		pset_2.Part1_2()
	case "Part 1.3 - Centralised server protocol":
		pset_2.Part1_3()
	case "Part 2   - Performance comparison":
		pset_2.Part2()
	}

}
