package main

import (
	"fmt"
	"io/ioutil"
	"strings"
)

func main() {
	fmt.Println("skeleton")

	buff := []string{}
	buff = append(buff, "package bookkeeping")
	b, _ := ioutil.ReadFile("../../data/bookkeeping/genesis.go")
	s := string(b)
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		if strings.Contains(line, "func ") {
			buff = append(buff, line)
			buff = append(buff, "}")
		}
	}

	ioutil.WriteFile("../../fresh-start/data/bookkeeping/genesis.go",
		[]byte(strings.Join(buff, "\n")), 0755)

}
