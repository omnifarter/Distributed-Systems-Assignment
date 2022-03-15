package Pset1

import (
	part1 "DistributedSystemsAssignment/Pset/Pset1/Part1"
	part2 "DistributedSystemsAssignment/Pset/Pset1/Part2"
)

func Part1_1() {
	part1.Part1_1()
}

func Part1_2() {
	part1.Part1_2()
}

func Part1_3() {
	part1.Part1_3()
}

func Part2_1(scenario string) {
	if scenario == "BEST" {
		part2.Part2_1_BEST()
	} else {
		part2.Part2_1_WORST()
	}
}

func Part2_2_a() {
	part2.Part2_2_a()
}
func Part2_2_b() {
	part2.Part2_2_b()
}
func Part2_3() {
	part2.Part2_3()
}
func Part2_4() {
	part2.Part2_4()
}
