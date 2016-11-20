package client

import (
	"strings"
	"unicode/utf8"

	"github.com/reiver/go-oi"
	"github.com/reiver/go-telnet"

	"bytes"
	"fmt"
	"spatialdb/index"
	"spatialdb/model"
	"strconv"
)

var skipRunes map[rune]bool

// ConnectionHandler is TELNET connection handler which parses db commands.
type ConnectionHandler struct {
	state *state
}

// ServeTELNET implements Handler interface for ConnectionHandler.
func (h *ConnectionHandler) ServeTELNET(ctx telnet.Context, w telnet.Writer, r telnet.Reader) {
	skipRunes := map[rune]bool{'\n': true, '\r': true, ';': true}

	var buffer [1]byte
	p := buffer[:]
	if h.state == nil {
		h.state = &state{tree: index.NewTree()}
	}

	// Append buffer to a command until ';' met.
	command := []rune{}
	for {
		n, err := r.Read(p)

		var r rune
		if n > 0 {
			// Buffer is of length 1, ignore the size.
			r, _ = utf8.DecodeRune(p[:n])
			if _, contains := skipRunes[r]; !contains {
				command = append(command, r)
			}
		}
		if delim, _ := utf8.DecodeRuneInString(";"); delim == r {
			oi.LongWriteString(w, h.processCommand(string(command))+"\n")
			command = []rune{}
		}
		if nil != err || r == utf8.RuneError {
			oi.LongWriteString(w, "Closing...\n")
			break
		}
	}
}

func (h *ConnectionHandler) processCommand(command string) string {
	parts := strings.Split(command, " ")
	switch parts[0] {
	case "add":
		p := &model.Point{Name: parts[1]}
		var err error
		p.Latitude, err = strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		p.Longitude, err = strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		h.state.tree.Insert(p)
		return fmt.Sprintf("Inserted %+v", p)
	case "print":
		buf := bytes.NewBuffer([]byte{})
		if err := h.state.tree.Serialize(buf); err == nil {
			return string(buf.Bytes())
		} else {
			return fmt.Sprintf("%v", err)
		}
	}
	return fmt.Sprintf("Unrecognized command: %s", command)
}
