package main

import (
	"flag"
	"fmt"
)

var (
	collationDisable bool
)

func init() {
	flag.BoolVar(&collationDisable, "collation-disable", false, "run collation related-test with new-collation disabled")
}

func main() {
	flag.Parse()
	if !collationDisable {
		fmt.Println(11111)
	} else {
		fmt.Println(collationDisable)
		fmt.Println(2222)
	}
}
