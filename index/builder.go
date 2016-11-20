package index

import (
	"bufio"
	"io"

	"encoding/json"

	"github.com/dhconnelly/rtreego"
)

// Deserialize builds tree from a given reader.
func Deserialize(reader *bufio.Reader) (*rtreego.Rtree, error) {
	bs, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	rt := &rtreego.Rtree{}
	err = json.Unmarshal(bs, rt)
	return rt, err
}

// Serialize stores tree in a given reader.
func (t *RTree) Serialize(writer io.Writer) error {
	if tJSON, err := json.Marshal(t.rtree); err == nil {
		writer.Write(tJSON)
		return nil
	} else {
		return err
	}
}
