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
	//b, _ := ioutil.ReadFile("../../data/bookkeeping/genesis.go")
	b, _ := ioutil.ReadFile("../../protocol/hash.go")
	s := string(b)
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		if strings.Contains(line, "func ") {
			buff = append(buff, line)
			buff = append(buff, "}")
		} else if strings.Contains(line, "type ") {
			buff = append(buff, line)
			if strings.Contains(line, "{") {
				buff = append(buff, "}")
			}
		}
	}

	//ioutil.WriteFile("../../fresh-start/data/bookkeeping/genesis.go",
	//[]byte(strings.Join(buff, "\n")), 0755)
	//ioutil.WriteFile("../../fresh-start/protocol/hash.go",
	//[]byte(strings.Join(buff, "\n")), 0755)

}
