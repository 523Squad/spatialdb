package main

import (
	"spatialdb/client"

	"github.com/reiver/go-telnet"
)

// PORT is open to TELNET connection.
const PORT = ":3456"

func main() {
	handler := &client.ConnectionHandler{}

	err := telnet.ListenAndServe(PORT, handler)
	if nil != err {
		//@TODO: Handle this error better.
		panic(err)
	}
}
