package cli

import (
	"fmt"

	pset_3 "DistributedSystemsAssignment/Pset/Pset3"

	"github.com/manifoldco/promptui"
)

func Pset3() {
	// select Problem Set assignment parts
	prompt := promptui.Select{
		Label: "Select which part to simulate.",
		Items: []string{
			"Experiment 1 - No faults",
			"Experiment 2a - Primary CM fails randomly",
			"Experiment 2b - Primary CM restarts",
			"Experiment 3 - Multiple failures",
		},
	}
	_, result, err := prompt.Run()

	if err != nil {
		fmt.Println("Exiting...")
		return
	}

	switch result {
	case "Experiment 1 - No faults":
		pset_3.Experiment1()
	case "Experiment 2a - Primary CM fails randomly":
		pset_3.Experiment2a()
	case "Experiment 2b - Primary CM restarts":
		pset_3.Experiment2b()
	case "Experiment 3 - Multiple failures":
		pset_3.Experiment3()
	}
}
