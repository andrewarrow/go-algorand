package main

import (
	"fmt"
	"fresh/daemon/algod"
	"fresh/data/bookkeeping"
)

func main() {
	fmt.Println("fresh-start")
	genesis := bookkeeping.Genesis{}
	absolutePath := "."

	s := algod.Server{
		RootPath: absolutePath,
		Genesis:  genesis,
	}
	s.Initialize()
	fmt.Println(s)
	s.Start()

}
