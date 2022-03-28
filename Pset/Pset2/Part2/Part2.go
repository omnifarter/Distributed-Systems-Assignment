package Pset2_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/table"
)

func Part_2() {
	fmt.Println("** IMPORTANT: Fully run all previous parts first to obtain complete timings. **")
	part1, err := ioutil.ReadFile("./Pset/Pset2/Part1/Part1.1.txt")
	part2, err := ioutil.ReadFile("./Pset/Pset2/Part1/Part1.2.txt")
	part3, err := ioutil.ReadFile("./Pset/Pset2/Part1/Part1.3.txt")

	if err != nil {
		log.Fatal(err)
	}

	part1_arr := strings.Split(string(part1), "\n")
	part2_arr := strings.Split(string(part2), "\n")
	part3_arr := strings.Split(string(part3), "\n")

	t := table.NewWriter()

	t.SetOutputMirror(os.Stdout)

	t.AppendHeader(table.Row{"No. of simultaneous requests", "Lamport's Shared Priority Queue", "Voting Protocol", "Centralized Server Protocol"})
	for i, val := range part1_arr {
		if i == 11 {
			break
		}
		t.AppendRow(table.Row{i + 1, val, part2_arr[i], part3_arr[i]})
	}
	t.Render()

}
