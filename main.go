package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/manifoldco/promptui"

	cli "DistributedSystemsAssignment/cli"
)

// StringPrompt asks for a string value using the label
func StringPrompt(label string) string {
	var s string
	r := bufio.NewReader(os.Stdin)
	for {
		fmt.Fprint(os.Stderr, label+" ")
		s, _ = r.ReadString('\n')
		if s != "" {
			break
		}
	}
	return strings.TrimSpace(s)
}

func main() {

	// Select Pset
	prompt := promptui.Select{
		Label: "Select Problem Set to test",
		Items: []string{"Problem Set 1", "Problem Set 2", "Problem Set 3"},
	}
	_, result, err := prompt.Run()

	if err != nil {
		fmt.Println("Exiting...")
		return
	}

	switch result {
	case "Problem Set 1":
		cli.Pset1()

	case "Problem Set 2":
		cli.Pset2()

	case "Problem Set 3":
		cli.Pset3()

	}

}
