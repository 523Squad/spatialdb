package main

import (
	"log"
	"os"
	"spatialdb/client"

	"time"

	"github.com/reiver/go-telnet"
)

// PORT is open to TELNET connection.
const PORT = ":3456"

func main() {
	f, err := os.OpenFile("main.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println(time.Now().String())

	handler := client.NewHandler()

	err = telnet.ListenAndServe(PORT, handler)
	if nil != err {
		//@TODO: Handle this error better.
		panic(err)
	}
}
