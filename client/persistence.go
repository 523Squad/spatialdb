package client

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/dhconnelly/rtreego"

	"fmt"
	"math/rand"
	"spatialdb/model"
	"strconv"
	"strings"
	"time"
)

const metaFilename = "meta.db"
const indexFilename = "index.db"
const recordFilename = "records.db"

// FileIO implements convenient operations for db file IO
type FileIO struct {
	fileMx       *sync.RWMutex
	priorWriteMx *sync.Mutex
	state        *state
}

func newReader() *FileIO {
	return &FileIO{fileMx: &sync.RWMutex{}, priorWriteMx: &sync.Mutex{}, state: &state{}}
}

func (f *FileIO) loadState() error {
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	err := f.loadTree()
	if err != nil {
		return err
	}
	log.Println("Successfully loaded index tree")
	err = f.loadMeta()
	if err == nil {
		log.Println("Successfully loaded metadata")
	}
	return err
}

func (f *FileIO) loadStateClient(out chan string) {
	err := f.loadState()
	if err == nil {
		out <- "Successfully loaded state"
	} else {
		out <- err.Error()
	}
}

func (f *FileIO) saveState(blocking bool) error {
	if blocking {
		f.priorWriteMx.Lock()
		f.fileMx.Lock()
		f.priorWriteMx.Unlock()
		defer f.fileMx.Unlock()
	}

	err := f.saveTree(f.state.tree, indexFilename, false)
	if err != nil {
		return err
	}
	log.Println("Successfully saved index tree")
	err = f.saveMeta(f.state, metaFilename, false)
	if err == nil {
		log.Println("Successfully saved metadata")
	}
	return err
}

func (f *FileIO) saveStateClient(out chan string) {
	err := f.saveState(true)
	if err == nil {
		out <- "Successfully saved state"
	} else {
		out <- err.Error()
	}
}

func (f *FileIO) loadTree() error {
	var file *os.File

	var err error
	var scanner *bufio.Scanner
	if file, err = os.OpenFile(indexFilename, os.O_RDONLY, 0660); err == nil {
		scanner = bufio.NewScanner(file)
	} else {
		return err
	}

	var tree *rtreego.Rtree
	if scanner.Scan(); err == nil {
		err = json.Unmarshal(scanner.Bytes(), &tree)
	}
	if tree == nil {
		tree = rtreego.NewTree(2, 3, 3)
		err = nil
	}
	if err != nil {
		return err
	}
	f.state.tree = tree

	return err
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

func (f *FileIO) loadMeta() error {
	var file *os.File
	var err error

	var scanner *bufio.Scanner
	if file, err = os.OpenFile(metaFilename, os.O_RDONLY, 0660); err == nil {
		scanner = bufio.NewScanner(file)
		if scanner.Scan(); err == nil {
			parts := strings.Split(scanner.Text(), " ")
			f.state.fileLen, err = strconv.ParseInt(parts[0], 10, 64)
			if err == nil {
				f.state.lastID, err = strconv.ParseInt(parts[1], 10, 64)
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

func (f *FileIO) createRecord(point *model.Point, recordFilename string) (int64, error) {
	var file *os.File
	var err error

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

func (f *FileIO) createRecordClient(p *model.Point, out chan string) {
	// Lock access here to ensure consistency of state data
	f.priorWriteMx.Lock()
	f.fileMx.Lock()
	f.priorWriteMx.Unlock()
	defer f.fileMx.Unlock()
	p.ID = f.state.lastID + 1
	p.Location.Offset = f.state.fileLen
	newSize, err := f.createRecord(p, recordFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	f.state.fileLen = newSize
	f.state.lastID++
	f.state.tree.Insert(p.Location)
	f.saveState(false)
	out <- fmt.Sprintf("Inserted %+v, new file size: %v", p.Location, newSize)
}

func (f *FileIO) readRecords(offsets []int64, blocking bool) ([]*model.Point, error) {
	var file *os.File
	var err error
	if blocking {
		f.fileMx.RLock()
		defer f.fileMx.RUnlock()
	}

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
func (f *FileIO) updateRecord(offset int64, args map[string]string, out chan string) {
	fileSuffix := "-" + strconv.FormatInt(rand.Int63(), 16)
	stateCopy := &state{}
	recordCopyFilename := "records" + fileSuffix + ".db"
	log.Printf("Copying records to %s...\n", recordCopyFilename)

	// Acquire priority write mutex to prevent other threads from writing data.
	f.priorWriteMx.Lock()
	defer f.priorWriteMx.Unlock()
	log.Println("Update got the priority lock")
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()
	log.Println("Update got the read lock")

	deletedBytes, err := f.copyRecordsWithout(recordCopyFilename, offset, stateCopy)
	if err != nil {
		out <- err.Error()
		return
	}
	var updated *model.Point
	err = json.Unmarshal(deletedBytes, &updated)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied records successfully, point to update: %+v\n", updated)

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
	newSize, err := f.createRecord(updated, recordCopyFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	stateCopy.fileLen = newSize

	indexCopyFilename := "index" + fileSuffix + ".db"
	err = f.indexCopy(recordCopyFilename, indexCopyFilename, stateCopy)
	if err != nil {
		out <- err.Error()
		return
	}

	metaCopyFilename := "meta" + fileSuffix + ".db"
	err = f.saveMeta(stateCopy, metaCopyFilename, false)
	if err != nil {
		out <- err.Error()
		return
	}

	// Release read-lock
	f.fileMx.RUnlock()
	// Add extra lock to keep it even
	defer f.fileMx.RLock()
	// This should be done in priority because of priority write mutex.
	f.fileMx.Lock()
	log.Println("Update got the write lock")
	defer f.fileMx.Unlock()

	err = os.Rename(recordCopyFilename, recordFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", recordCopyFilename, recordFilename)
	err = os.Rename(indexCopyFilename, indexFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", indexCopyFilename, indexFilename)
	// Lock state update too
	err = os.Rename(metaCopyFilename, metaFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", metaCopyFilename, metaFilename)
	*f.state = *stateCopy
	log.Printf("New state: %+v\n", f.state)
	out <- fmt.Sprintf("Successfully updated %+v at offset %d", updated, offset)
}

// Creates copy of records.db without given record, rebuilds index and metadata
// of copy and then replaces original files with new ones new files
func (f *FileIO) deleteRecord(offset int64, out chan string) {
	fileSuffix := "-" + strconv.FormatInt(rand.Int63(), 16)
	stateCopy := &state{}
	recordCopyFilename := "records" + fileSuffix + ".db"
	log.Printf("Copying records to %s...\n", recordCopyFilename)

	// Acquire priority write mutex to prevent other threads from writing data.
	f.priorWriteMx.Lock()
	defer f.priorWriteMx.Unlock()

	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	deletedBytes, err := f.copyRecordsWithout(recordCopyFilename, offset, stateCopy)
	if err != nil {
		out <- err.Error()
		return
	}
	var deleted *model.Point
	err = json.Unmarshal(deletedBytes, &deleted)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied records successfully, deleted point is %+v", deleted)

	indexCopyFilename := "index" + fileSuffix + ".db"
	err = f.indexCopy(recordCopyFilename, indexCopyFilename, stateCopy)
	if err != nil {
		out <- err.Error()
		return
	}

	metaCopyFilename := "meta" + fileSuffix + ".db"
	err = f.saveMeta(stateCopy, metaCopyFilename, false)
	if err != nil {
		out <- err.Error()
		return
	}

	// Release read-lock
	f.fileMx.RUnlock()
	defer f.fileMx.RLock()
	// This should be done in priority because of priority write mutex.
	f.fileMx.Lock()
	defer f.fileMx.Unlock()

	err = os.Rename(recordCopyFilename, recordFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", recordCopyFilename, recordFilename)
	err = os.Rename(indexCopyFilename, indexFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", indexCopyFilename, indexFilename)
	// Lock state update too
	err = os.Rename(metaCopyFilename, metaFilename)
	if err != nil {
		out <- err.Error()
		return
	}
	log.Printf("Copied %s to %s\n", metaCopyFilename, metaFilename)
	*f.state = *stateCopy
	log.Printf("New state: %+v\n", f.state)
	out <- fmt.Sprintf("Successfully deleted %+v at offset %d", deleted, offset)
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

	time.Sleep(time.Duration(10) * time.Second)

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

func (f *FileIO) searchIntersect(rect *rtreego.Rect, out chan string) {
	f.fileMx.RLock()
	defer f.fileMx.RUnlock()

	data := f.state.tree.SearchIntersect(rect)
	offsets := []int64{}
	for _, spatial := range data {
		if spoint, ok := spatial.(*rtreego.SPoint); ok {
			offsets = append(offsets, spoint.Offset)
		}
	}
	sort.Sort(model.Int64Slice(offsets))
	if records, err := f.readRecords(offsets, false); err == nil {
		res := fmt.Sprintf("Found records %d:", len(records))
		for _, r := range records {
			res = res + fmt.Sprintf("\n\t%+v", *r)
		}
		out <- res
	} else {
		out <- err.Error()
	}
}
