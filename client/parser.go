package client

import (
	"strings"
	"unicode/utf8"

	"github.com/reiver/go-oi"
	"github.com/reiver/go-telnet"

	"bufio"
	"bytes"
	"fmt"
	"os"
	"spatialdb/index"
	"spatialdb/model"
	"strconv"
	"time"
)

const filename = "spatial.db"

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
	case "save":
		var file *os.File
		var err error
		if file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0660); err == nil {
			err = h.state.tree.Serialize(file)
		}
		if err != nil {
			err = file.Sync()
		}
		if err == nil {
			return "Successfully saved"
		}
		return fmt.Sprintf("%v", err)
	case "load":
		var file *os.File
		var err error
		if file, err = os.OpenFile(filename, os.O_RDONLY, 0660); err == nil {
			err = h.state.tree.Deserialize(bufio.NewReader(file))
		}
		if err == nil {
			return "Successfully loaded"
		}
		return fmt.Sprintf("%v", err)
	case "hang":
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
		}
		return "Hung!"
	}
	return fmt.Sprintf("Unrecognized command: %s", command)
}
