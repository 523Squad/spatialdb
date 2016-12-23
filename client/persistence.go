package client

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"

	"github.com/dhconnelly/rtreego"

	"fmt"
	"math/rand"
	"spatialdb/model"
	"strconv"
	"strings"
)

const metaFilename = "meta.db"
const indexFilename = "index.db"
const recordFilename = "records.db"

// FileIO implements convenient operations for db file IO
type FileIO struct {
	fileMx       *sync.RWMutex
	priorWriteMx *sync.Mutex
}

func newReader() *FileIO {
	return &FileIO{fileMx: &sync.RWMutex{}, priorWriteMx: &sync.Mutex{}}
}

func (f *FileIO) loadTree() (*rtreego.Rtree, error) {
	var file *os.File
	// TODO: Separate reading and deserialization to reduce blocking
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	var err error
	var scanner *bufio.Scanner
	if file, err = os.OpenFile(indexFilename, os.O_RDONLY, 0660); err == nil {
		scanner = bufio.NewScanner(file)
	} else {
		return nil, err
	}

	var tree *rtreego.Rtree
	if scanner.Scan(); err == nil {
		err = json.Unmarshal(scanner.Bytes(), &tree)
	}

	if err == nil {
		return tree, nil
	}
	return nil, err
}

func (f *FileIO) saveTree(tree *rtreego.Rtree, indexFilename string, blocking bool) error {
	var file *os.File
	var err error
	if blocking {
		f.priorWriteMx.Lock()
		f.fileMx.Lock()
		f.priorWriteMx.Unlock()
		defer f.fileMx.Unlock()
	}
	// TODO: Update tree, not rewrite.
	if file, err = os.OpenFile(indexFilename, os.O_RDWR|os.O_CREATE, 0660); err == nil {
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

func (f *FileIO) saveTreeDefault(tree *rtreego.Rtree) error {
	return f.saveTree(tree, indexFilename, true)
}

func (f *FileIO) loadMeta(s *state) error {
	var file *os.File
	var err error
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	var scanner *bufio.Scanner
	if file, err = os.OpenFile(metaFilename, os.O_RDONLY, 0660); err == nil {
		scanner = bufio.NewScanner(file)
		if scanner.Scan(); err == nil {
			parts := strings.Split(scanner.Text(), " ")
			s.fileLen, err = strconv.ParseInt(parts[0], 10, 64)
			if err == nil {
				s.lastID, err = strconv.ParseInt(parts[1], 10, 64)
			}
		}
	}
	return err
}

func (f *FileIO) saveMeta(s *state, filename string, blocking bool) error {
	var file *os.File
	var err error
	// TODO: Come up with more elegant workaround
	if blocking {
		f.priorWriteMx.Lock()
		f.fileMx.Lock()
		f.priorWriteMx.Unlock()
		defer f.fileMx.Unlock()
	}
	if file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660); err == nil {
		_, err = file.WriteString(fmt.Sprintf("%d %d \n", s.fileLen, s.lastID))
	}
	if err == nil {
		err = file.Sync()
	}
	log.Printf("Successfully saved meta with %d %d\n", s.fileLen, s.lastID)
	return err
}

func (f *FileIO) saveMetaDefault(s *state) error {
	return f.saveMeta(s, metaFilename, true)
}

func (f *FileIO) createRecord(point *model.Point, recordFilename string, blocking bool) (int64, error) {
	var file *os.File
	var err error

	if blocking {
		f.priorWriteMx.Lock()
		f.fileMx.Lock()
		f.priorWriteMx.Unlock()
		defer f.fileMx.Unlock()
	}

	newSize := int64(-1)
	if file, err = os.OpenFile(recordFilename, os.O_RDWR|os.O_APPEND, 0660); err == nil {
		if js, err := json.Marshal(point); err == nil {
			_, err = file.WriteString(string(js) + "\n")
		}
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		if stat, err := file.Stat(); err == nil {
			newSize = stat.Size()
		}
	}
	return newSize, err
}

func (f *FileIO) createRecordDefault(point *model.Point) (int64, error) {
	return f.createRecord(point, recordFilename, true)
}

func (f *FileIO) readRecords(offsets []int64) ([]*model.Point, error) {
	var file *os.File
	var err error
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	var reader *bufio.Reader
	if file, err = os.OpenFile(recordFilename, os.O_RDONLY, 0660); err == nil {
		reader = bufio.NewReader(file)
	} else {
		return nil, err
	}

	points := []*model.Point{}
	bytePointer := int64(0)
	for _, offset := range offsets {
		log.Printf("Reading offset %d, discarding %d...\n", offset, offset-bytePointer)
		if _, err := reader.Discard(int(offset - bytePointer)); err == nil {
			p, bytesRead, err := f.readRecord(reader)
			if err != nil {
				break
			}
			points = append(points, p)
			bytePointer = offset + int64(bytesRead)
		} else {
			break
		}
	}
	if err == nil {
		return points, nil
	}
	return nil, err
}

// Reader must read from blocked file.
func (f *FileIO) readRecord(reader *bufio.Reader) (*model.Point, int, error) {
	var err error
	if line, err := reader.ReadString('\n'); err == nil {
		log.Printf("Line read: %s", line)
		var p *model.Point
		if err = json.Unmarshal([]byte(line), &p); err == nil {
			// Line includes '\n'
			return p, len(line), nil
		}
	}
	return nil, 0, err
}

// TODO: Refactor to unify with deleteRecord()
func (f *FileIO) updateRecord(offset int64, args map[string]string, s *state) (*model.Point, error) {
	fileSuffix := "-" + strconv.FormatInt(rand.Int63(), 16)
	stateCopy := &state{}
	recordCopyFilename := "records" + fileSuffix + ".db"
	log.Printf("Copying records to %s...\n", recordCopyFilename)

	// Acquire priority write mutex to prevent other threads from writing data.
	f.priorWriteMx.Lock()
	defer f.priorWriteMx.Unlock()

	f.fileMx.RLock()

	deletedBytes, err := f.copyRecordsWithout(recordCopyFilename, offset, stateCopy)
	if err != nil {
		return nil, err
	}
	var updated *model.Point
	err = json.Unmarshal(deletedBytes, &updated)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied records successfully, point to update: %+v", updated)

	for key, value := range args {
		switch strings.ToLower(key) {
		case "name":
			updated.Name = value
		case "lat":
			lat, err := strconv.ParseFloat(value, 64)
			if err != nil {
				break
			}
			updated.Location.Latitude = lat
		case "lng":
			lng, err := strconv.ParseFloat(value, 64)
			if err != nil {
				break
			}
			updated.Location.Longitude = lng
		}
	}

	updated.Location.Offset = stateCopy.fileLen
	log.Printf("Parsed arguments, point to update: %+v", updated)
	newSize, err := f.createRecord(updated, recordCopyFilename, false)
	if err != nil {
		return nil, err
	}
	stateCopy.fileLen = newSize

	indexCopyFilename := "index" + fileSuffix + ".db"
	err = f.indexCopy(recordCopyFilename, indexCopyFilename, stateCopy)
	if err != nil {
		return nil, err
	}

	metaCopyFilename := "meta" + fileSuffix + ".db"
	err = f.saveMeta(stateCopy, metaCopyFilename, false)
	if err != nil {
		return nil, err
	}

	// Release read-lock
	f.fileMx.RUnlock()
	// This should be done in priority because of priority write mutex.
	f.fileMx.Lock()
	defer f.fileMx.Unlock()

	err = os.Rename(recordCopyFilename, recordFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", recordCopyFilename, recordFilename)
	err = os.Rename(indexCopyFilename, indexFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", indexCopyFilename, indexFilename)
	// Lock state update too
	err = os.Rename(metaCopyFilename, metaFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", metaCopyFilename, metaFilename)
	*s = *stateCopy
	log.Printf("New state: %+v", s)
	return updated, nil
}

// Creates copy of records.db without given record, rebuilds index and metadata
// of copy and then replaces original files with new ones new files
func (f *FileIO) deleteRecord(offset int64, s *state) (*model.Point, error) {
	fileSuffix := "-" + strconv.FormatInt(rand.Int63(), 16)
	stateCopy := &state{}
	recordCopyFilename := "records" + fileSuffix + ".db"
	log.Printf("Copying records to %s...\n", recordCopyFilename)

	// Acquire priority write mutex to prevent other threads from writing data.
	f.priorWriteMx.Lock()
	defer f.priorWriteMx.Unlock()

	f.fileMx.RLock()

	deletedBytes, err := f.copyRecordsWithout(recordCopyFilename, offset, stateCopy)
	if err != nil {
		return nil, err
	}
	var deleted *model.Point
	err = json.Unmarshal(deletedBytes, &deleted)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied records successfully, deleted point is %+v", deleted)

	indexCopyFilename := "index" + fileSuffix + ".db"
	err = f.indexCopy(recordCopyFilename, indexCopyFilename, stateCopy)
	if err != nil {
		return nil, err
	}

	metaCopyFilename := "meta" + fileSuffix + ".db"
	err = f.saveMeta(stateCopy, metaCopyFilename, false)
	if err != nil {
		return nil, err
	}

	// Release read-lock
	f.fileMx.RUnlock()
	// This should be done in priority because of priority write mutex.
	f.fileMx.Lock()
	defer f.fileMx.Unlock()

	err = os.Rename(recordCopyFilename, recordFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", recordCopyFilename, recordFilename)
	err = os.Rename(indexCopyFilename, indexFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", indexCopyFilename, indexFilename)
	// Lock state update too
	err = os.Rename(metaCopyFilename, metaFilename)
	if err != nil {
		return nil, err
	}
	log.Printf("Copied %s to %s\n", metaCopyFilename, metaFilename)
	*s = *stateCopy
	log.Printf("New state: %+v", s)
	return deleted, err
}

func (f *FileIO) copyRecordsWithout(destFilename string, offset int64, s *state) ([]byte, error) {
	var originFile, destFile *os.File
	var err error

	var scanner *bufio.Scanner
	if originFile, err = os.OpenFile(recordFilename, os.O_RDONLY, 0660); err == nil {
		scanner = bufio.NewScanner(originFile)
	} else {
		return nil, err
	}

	if destFile, err = os.OpenFile(destFilename, os.O_WRONLY|os.O_CREATE, 0660); err != nil {
		return nil, err
	}

	bytePointer := int64(0)
	var deletedRecord []byte
	var deletedLen int

	for scanner.Scan() {
		// Ok, so ReadString() accounts for separator, Scan() doesn't.
		data := append(scanner.Bytes(), '\n')
		log.Printf("Reading from original records file: %s", string(data))
		if bytePointer < offset {
			log.Printf("Writing to records copy file: %s", string(data))
			destFile.Write(data)
		} else if bytePointer == offset {
			log.Printf("Deleted data is %s", string(data))
			deletedRecord = data[:]
			deletedLen = len(data)
		} else if bytePointer > offset {
			var p *model.Point
			if err := json.Unmarshal(data, &p); err == nil {
				p.Location.Offset = bytePointer - int64(deletedLen)
				if data, err = json.Marshal(p); err == nil {
					data = append(data, '\n')
					log.Printf("Writing to records copy file: %s", string(data))
					destFile.Write(data)
				}
			}
		}
		bytePointer = bytePointer + int64(len(data))
	}

	err = scanner.Err()
	if err != nil {
		return nil, err
	}

	stat, err := destFile.Stat()
	if err != nil {
		return nil, err
	}

	s.fileLen = stat.Size()
	return deletedRecord, nil
}

// Returns last ID to for metadata.
func (f *FileIO) indexCopy(recordFilename, indexFilename string, s *state) error {
	err := f.buildIndexCopy(recordFilename, s)
	if err != nil {
		return err
	}
	log.Println("Successfully built index copy")

	err = f.saveTree(s.tree, indexFilename, false)
	if err != nil {
		return err
	}
	log.Printf("Successfully saved index copy at %s\n", indexFilename)
	return nil
}

func (f *FileIO) buildIndexCopy(filename string, s *state) error {
	var file *os.File
	var err error

	var reader *bufio.Reader
	if file, err = os.OpenFile(filename, os.O_RDONLY, 0660); err == nil {
		reader = bufio.NewReader(file)
	} else {
		return err
	}

	// TODO: Create new instance at the single point.
	tree := rtreego.NewTree(2, 3, 3)
	bytePointer := int64(0)
	lastID := int64(-1)
	for {
		p, bytesRead, err := f.readRecord(reader)
		if (err != nil && err != io.EOF) || p == nil {
			break
		}

		p.Location.Offset = bytePointer
		tree.Insert(p.Location)
		if lastID < p.ID {
			lastID = p.ID
		}

		bytePointer += int64(bytesRead)
	}
	if err == nil || err == io.EOF {
		s.tree = tree
		s.lastID = lastID
		return nil
	}
	return err
}
