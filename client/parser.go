package client

import (
	"unicode/utf8"

	"github.com/reiver/go-oi"
	"github.com/reiver/go-telnet"
)

// ConnectionHandler is TELNET connection handler which parses db commands.
type ConnectionHandler struct{}

// ServeTELNET implements Handler interface for ConnectionHandler.
func (h *ConnectionHandler) ServeTELNET(ctx telnet.Context, w telnet.Writer, r telnet.Reader) {
	var buffer [1]byte
	p := buffer[:]

	// Append buffer to a command until ';' met.
	command := []rune{}
	for {
		n, err := r.Read(p)

		var r rune
		if n > 0 {
			// Buffer is of length 1, ignore the size.
			r, _ = utf8.DecodeRune(p[:n])
			command = append(command, r)
		}
		if end, _ := utf8.DecodeRuneInString(";"); end == r {
			oi.LongWriteString(w, string(command)+"\n")
			command = []rune{}
		}
		if nil != err || r == utf8.RuneError {
			break
		}
	}
}
