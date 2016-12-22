package client

import (
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/dhconnelly/rtreego"
	"github.com/reiver/go-oi"
	"github.com/reiver/go-telnet"

	"encoding/json"
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
	state *state
}

func NewHandler() *ConnectionHandler {
	return &ConnectionHandler{fileIO: newReader()}
}

// ServeTELNET implements Handler interface for ConnectionHandler.
func (h *ConnectionHandler) ServeTELNET(ctx telnet.Context, w telnet.Writer, r telnet.Reader) {
	skipRunes := map[rune]bool{'\n': true, '\r': true, ';': true}

	var buffer [1]byte
	p := buffer[:]

	conn := &connection{state: &state{tree: rtreego.NewTree(2, 3, 3)}}
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
			oi.LongWriteString(w, h.processCommand(conn, string(command))+"\n")
			command = []rune{}
		}
		if nil != err || r == utf8.RuneError {
			oi.LongWriteString(w, "Closing...\n")
			break
		}
	}
}

func (h *ConnectionHandler) processCommand(c *connection, command string) string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in processCommand", r)
		}
	}()
	parts := strings.Split(command, " ")
	var err error
	var res string
	switch parts[0] {
	case "add":
		p := &model.Point{Name: parts[1]}
		var err error
		lat, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			break
		}
		lng, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			break
		}
		p.Location = &rtreego.SPoint{Latitude: lat, Longitude: lng, Offset: c.state.fileLen}
		newSize, err := h.fileIO.createRecord(p)
		if err != nil {
			break
		}
		c.state.fileLen = newSize
		c.state.tree.Insert(p.Location)
		res = fmt.Sprintf("Inserted %+v, new file size: %v", p.Location, c.state.fileLen)
	case "intersect":
		var err error
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
		res = fmt.Sprintf("%+v", rect)
		data := c.state.tree.SearchIntersect(rect)
		res = fmt.Sprintf("Found data: %+v\n", data)
		offsets := []int64{}
		for _, spatial := range data {
			if spoint, ok := spatial.(*rtreego.SPoint); ok {
				offsets = append(offsets, spoint.Offset)
			}
		}
		sort.Sort(model.Int64Slice(offsets))
		if records, err := h.fileIO.readRecords(offsets); err == nil {
			res = res + fmt.Sprintf("Found records %d:\n", len(records))
			for _, r := range records {
				res = res + fmt.Sprintf("\t%+v\n", *r)
			}
		}
	case "print":
		js, err := json.Marshal(c.state.tree)
		if err == nil {
			res = string(js)
		}
	case "save":
		err = h.fileIO.saveTree(c.state.tree)
		if err == nil {
			res = "Successfully saved"
		}
	case "load":
		c.state.tree, err = h.fileIO.loadTree()
		if err != nil {
			break
		}
		c.state.fileLen, err = h.fileIO.recordsLen()
		if err == nil {
			res = "Successfully loaded"
		}
	case "hang":
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
		}
		res = "Hung!"
	default:
		res = fmt.Sprintf("Unrecognized command: %s", command)
	}
	if err == nil {
		return res
	}
	return fmt.Sprintf("%v", err)
}
