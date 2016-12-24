package client

import (
	"strings"
	"unicode/utf8"

	"github.com/dhconnelly/rtreego"
	"github.com/reiver/go-oi"
	"github.com/reiver/go-telnet"

	"fmt"
	"spatialdb/model"
	"strconv"
	"time"
)

const filename = "spatial.db"

var skipRunes map[rune]bool

// ConnectionHandler is TELNET connection handler which parses db commands.
type ConnectionHandler struct {
	fileIO *FileIO
}

type connection struct {
	out chan string
}

// NewHandler creates new connection handler for telnet clients.
func NewHandler() *ConnectionHandler {
	return &ConnectionHandler{fileIO: newReader()}
}

// ServeTELNET implements Handler interface for ConnectionHandler.
func (h *ConnectionHandler) ServeTELNET(ctx telnet.Context, w telnet.Writer, r telnet.Reader) {
	skipRunes := map[rune]bool{'\n': true, '\r': true, ';': true}

	var buffer [1]byte
	p := buffer[:]

	out := make(chan string)
	conn := &connection{out: out}
	go h.listenToOutput(w, out)
	h.processCommand(conn, "load")
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
			h.processCommand(conn, string(command))
			command = []rune{}
		}
		if nil != err || r == utf8.RuneError {
			oi.LongWriteString(w, "Closing...\n")
			break
		}
	}
}

func (h *ConnectionHandler) listenToOutput(w telnet.Writer, out chan string) {
	for msg := range out {
		oi.LongWriteString(w, msg+"\n")
	}
}

func (h *ConnectionHandler) processCommand(c *connection, command string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in processCommand", r)
		}
	}()
	parts := strings.Split(command, " ")
	var err error
	switch parts[0] {
	case "add":
		p := &model.Point{Name: parts[1]}
		lat, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			break
		}
		lng, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			break
		}
		p.Location = &rtreego.SPoint{Latitude: lat, Longitude: lng}
		go h.fileIO.createRecordClient(p, c.out)
	case "intersect":
		lat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			break
		}
		lng, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			break
		}
		width, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			break
		}
		height, err := strconv.ParseFloat(parts[4], 64)
		if err != nil {
			break
		}
		rect, err := rtreego.NewRect(rtreego.Point{lat, lng}, []float64{width, height})
		if err != nil {
			break
		}
		go h.fileIO.searchIntersect(rect, c.out)
	case "update":
		// Currently use offset, but TODO: add simple binary tree index for model.Point.ID
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			break
		}

		args := map[string]string{}
		// Assume operation is `set``
		for i := 2; i < len(parts)-1; i += 2 {
			args[parts[i]] = parts[i+1]
		}
		go h.fileIO.updateRecord(offset, args, c.out)
	case "delete":
		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			break
		}
		go h.fileIO.deleteRecord(offset, c.out)
	case "save":
		go h.fileIO.saveStateClient(c.out)
	case "load":
		go h.fileIO.loadStateClient(c.out)
	case "hang":
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
		}
		c.out <- "Hung!"
	default:
		c.out <- fmt.Sprintf("Unrecognized command: %s", command)
	}
	if err != nil {
		c.out <- err.Error()
	}
}
