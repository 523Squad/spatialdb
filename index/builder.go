package index

import (
	"bufio"
	"io"

	"encoding/json"
)

// Deserialize builds tree from a given reader.
func (t *RTree) Deserialize(reader *bufio.Reader) error {
	bs, _, err := reader.ReadLine()
	if err != nil {
		return err
	}
	err = json.Unmarshal(bs, &t)
	return err
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
