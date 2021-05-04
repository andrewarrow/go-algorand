package main

import (
	"fmt"
	"fresh/algod"
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
	fmt.Println(s)
	s.Start()

}
