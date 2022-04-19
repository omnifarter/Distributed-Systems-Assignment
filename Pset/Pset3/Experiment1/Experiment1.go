package pset3

import (
	FTIvy "DistributedSystemsAssignment/Pset/Pset3/FTIvy"
	ivy "DistributedSystemsAssignment/Pset/Pset3/Ivy"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/table"
)

func DoExperiment1() {
	ivy.Experiment1()
	FTIvy.Experiment1()

	ivy, err := ioutil.ReadFile("./Pset/Pset3/Experiment1/ivy_timings.txt")
	ftivy, err := ioutil.ReadFile("./Pset/Pset3/Experiment1/ft_ivy_timings.txt")

	if err != nil {
		log.Fatal(err)
	}

	readIvy := strings.Split(strings.Split(strings.Split(string(ivy), "---WRITE REQUESTS---\n")[0], "---READ REQUESTS---\n")[1], "\n")
	writeIvy := strings.Split(strings.Split(string(ivy), "---WRITE REQUESTS---\n")[1], "\n")
	readFTIvy := strings.Split(strings.Split(strings.Split(string(ftivy), "---WRITE REQUESTS---\n")[0], "---READ REQUESTS---\n")[1], "\n")
	writeFTIvy := strings.Split(strings.Split(string(ftivy), "---WRITE REQUESTS---\n")[1], "\n")

	t := table.NewWriter()

	t.SetOutputMirror(os.Stdout)

	t.AppendHeader(table.Row{"Request from Node", "Read Ivy", "Read Fault Tolerant Ivy", "Write Ivy", "Write Fault Tolerant Ivy"})
	for i, val := range readIvy {
		if i == 8 {
			break
		}
		t.AppendRow(table.Row{i + 1, val, readFTIvy[i], writeIvy[i], writeFTIvy[i]})
	}
	t.Render()

}
