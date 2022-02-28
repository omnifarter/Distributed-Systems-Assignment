package cli

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
			"Part 1.3 - Vector clock",
			"Part 2.1 - Bully algorithm, best case",
			"Part 2.1 - Bully algorithm, worst case",
			"Part 2.2a - Coordinator fails while announcing winning the election",
			"Part 2.2b - Random node fails before while coordinator is announcing the winner",
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
	case "Part 1.3 - Vector clock":
		pset_1.Part1_3()
	case "Part 2.1 - Bully algorithm, best case":
		pset_1.Part2_1("BEST")
	case "Part 2.1 - Bully algorithm, worst case":
		pset_1.Part2_1("WORST")
	case "Part 2.2a - Coordinator fails while announcing winning the election":
		pset_1.Part2_2_a()
	case "Part 2.2b - Random node fails before while coordinator is announcing the winner":
		pset_1.Part2_2_b()
	}
}
