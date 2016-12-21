package client

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/dhconnelly/rtreego"

	"spatialdb/model"
)

const indexFilename = "index.db"

// FileIO implements convenient operations for db file IO
type FileIO struct {
	flock *sync.RWMutex
}

func newReader() *FileIO {
	return &FileIO{flock: &sync.RWMutex{}}
}

func (f *FileIO) loadTree() (*rtreego.Rtree, error) {
	var file *os.File
	// TODO: Separate reading and deserialization to reduce blocking
	f.flock.RLock()
	defer f.flock.RUnlock()

	var err error
	var reader *bufio.Reader
	if file, err = os.OpenFile(indexFilename, os.O_RDONLY, 0660); err == nil {
		reader = bufio.NewReader(file)
	} else {
		return nil, err
	}

	var tree *rtreego.Rtree
	// TODO: Handle isPrefix case
	if line, _, err := reader.ReadLine(); err == nil {
		err = json.Unmarshal(line, &tree)
	}

	if err == nil {
		return tree, nil
	}
	return nil, err
}

func (f *FileIO) saveTree(tree *rtreego.Rtree) error {
	var file *os.File
	var err error
	f.flock.Lock()
	defer f.flock.Unlock()
	time.Sleep(10 * time.Second)
	// TODO: Update tree, not rewrite.
	if file, err = os.OpenFile(indexFilename, os.O_RDWR /*|os.O_APPEND*/, 0660); err == nil {
		if js, err := json.Marshal(tree); err == nil {
			_, err = file.WriteString(string(js) + "\n")
		}
		// TODO: Write everything else..
	}
	if err == nil {
		err = file.Sync()
	}
	return err
}

func (f *FileIO) loadRecord(offset int64) (*model.Point, error) {
	return nil, nil
}
