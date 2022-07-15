package durostore

import (
	"encoding"
	"io/fs"
	"os"
	"sync"

	internal "github.com/antonio-alexander/go-durostore/internal"

	flock "github.com/gofrs/flock"
	errors "github.com/pkg/errors"
)

//KIM: although it doesn't read well (I think), the logic will allow at least ONE write that exceeds any max size
//configured this is to prevent having to split indexes across files and to not have a problem where the bytes to
// write exceeds the size of an individual file (but not the max size) or the total max size

type durostore struct {
	sync.RWMutex                                 //mutex for threadsafe operations
	flock             *flock.Flock               //file locker
	config            Configuration              //the current configuration
	files             internal.Files             //files
	configured        bool                       //whether or not durostore is configured
	size              int64                      //the total data in-memory (in bytes)
	indexes           map[uint64]*internal.Index //map of Indexes organized by index
	data              map[uint64]*[]byte         //map of Data organized by index
	writeIndex        uint64                     //the index (to write)
	readIndex         uint64                     //the index (to read)
	writeFileDataSize int64                      //the current size of the write data file
}

//New can be used to generate a populated store pointer
// that implements Owner/Manager/Durostore
func New(parameters ...interface{}) interface {
	Owner
	Reader
	Writer
	Info
} {
	d := &durostore{
		indexes: make(map[uint64]*internal.Index),
		data:    make(map[uint64]*[]byte),
	}
	//it's better, to "create" and then "configure" IMO,
	// but now we get both with optional parameters...
	for _, p := range parameters {
		switch v := p.(type) {
		case Configuration:
			if err := d.Load(v); err != nil {
				//KIM: this is the only way to communicate an error/failure
				panic(err)
			}
		}
	}
	return d
}

func (d *durostore) fLock() {
	if d.config.FileLocking {
		if err := d.flock.Lock(); err != nil {
			panic(err)
		}
	}
}

func (d *durostore) fUnlock() {
	if d.config.FileLocking {
		if err := d.flock.Unlock(); err != nil {
			panic(err)
		}
	}
}

func (d *durostore) fRLock() {
	if d.config.FileLocking {
		//KIM: this is to simplify the implementation, errors should occur
		// catastrophically, and hence the panic is ok (I think)
		if err := d.flock.RLock(); err != nil {
			panic(err)
		}
	}
}

func (d *durostore) readIndexes(updateWriteIndex bool) (err error) {
	var writeIndex uint64

	//read all of the indexes, and determine the next read and write index, write index
	// will be incremented on the front end, while the read index is incremented on the back
	// end, if the readindex is 0, we increment it by 1 since 0 should never be a valid
	// index
	d.indexes, d.data = make(map[uint64]*internal.Index), make(map[uint64]*[]byte)
	if d.readIndex, writeIndex, err = internal.ReadIndexes(d.config.Directory, d.config.FilePrefix, d.indexes); err != nil {
		return
	}
	if updateWriteIndex {
		d.writeIndex = writeIndex
	}
	return d.readData()
}

func (d *durostore) readData() (err error) {
	var indexes []*internal.Index
	var dataSize int64

	//REVIEW: it would be super useful for this function to also
	// update the indexes (e.g. someone else is writing indexes)

	//attempt to read data, either loading ALL of the data
	// into memory or only loading up to the chunk size (in bytes)
	// after gathering all of the indexes, read the data in
	// and upsert into the stored data
	for i := d.readIndex; ; i++ {
		index, ok := d.indexes[i]
		if !ok || (d.config.MaxChunk > 0 && dataSize >= d.config.MaxChunk) {
			break
		}
		indexes = append(indexes, index)
		dataSize += (index.Finish - index.Start)
	}
	if err = internal.ReadData(d.data, indexes...); err != nil {
		return
	}
	if len(d.data) <= 0 && len(d.indexes) <= 0 {
		//KIM: since indexes are held in memory in total, if there's no data
		// AND no indexes, the store is completely empty, so we should "reset"
		// everything
		if d.files, d.writeFileDataSize, err = internal.FindFiles(d.config.Directory, d.config.FilePrefix); err != nil {
			return
		}
		d.writeIndex, d.readIndex = 0, 0
	}
	d.size = dataSize

	return
}

//Close can be used to set the internal pointers to nil and prepare the helper pointer/interface for
// garbage cleanup, Once executed, the service pointer cannot be re-used and may panic under certain
// circumstances
func (d *durostore) Close() (err error) {
	d.Lock()
	defer d.Unlock()

	//set internal pointers to nil
	//set configuration to defaults
	if !d.configured {
		return
	}
	if d.config.FileLocking {
		err = d.flock.Close()
	}
	d.indexes, d.data = make(map[uint64]*internal.Index), make(map[uint64]*[]byte)
	d.config = Configuration{}
	d.writeIndex, d.readIndex, d.size = 0, 0, 0
	d.configured = false

	return
}

//Load can be used to configure and initialize the business logic for a store, it will return a
// channel for errors since some of the enqueue interfaces can't communicate errors
func (d *durostore) Load(config Configuration) (err error) {
	d.Lock()
	defer d.Unlock()

	var info fs.FileInfo
	var f *os.File

	//check if the file/directory exists, if not, create
	// otherwise, ensure that it's a folder and you have
	// permissions, then create the duroStore pointer
	//store the configuration, resize the queue, then find the read/write files and
	// sizes, then read the indexes and read the data
	if f, err = os.Open(config.Directory); err != nil {
		if err = os.MkdirAll(config.Directory, 0700); err != nil {
			return errors.Wrap(err, ErrUnableMakeDir)
		}
	} else {
		defer f.Close()

		if info, err = f.Stat(); err != nil {
			return
		}
		if !info.IsDir() {
			return errors.Errorf(ErrNotDirectory, config.Directory)
		}
	}
	d.config = config
	d.configured = true
	d.indexes, d.data, d.size = make(map[uint64]*internal.Index), make(map[uint64]*[]byte), 0
	if d.files, d.writeFileDataSize, err = internal.FindFiles(d.config.Directory, d.config.FilePrefix); err != nil {
		return
	}
	if d.config.FileLocking {
		if d.flock != nil {
			d.flock.Close()
		}
		d.flock = flock.New(d.files.LockFile)
		d.fRLock()
		defer d.fUnlock()
	}
	if err = d.readIndexes(true); err != nil {
		return
	}

	return
}

func (d *durostore) PruneDirectory(indices ...uint64) (err error) {
	d.Lock()
	defer d.Unlock()

	var readIndexFile string

	if !d.configured {
		return errors.New(ErrNotConfigured)
	}
	if len(indices) > 0 {
		i := indices[0]
		index, ok := d.indexes[i]
		if !ok {
			return errors.New(ErrIndexNotFoundf)
		}
		readIndexFile = index.IndexFile
	}
	d.fLock()
	defer d.fUnlock()
	if err = internal.PruneDirectory(d.config.Directory, d.config.FilePrefix, readIndexFile); err != nil {
		return
	}
	return d.readIndexes(true)
}

func (d *durostore) UpdateIndexes(indexSize ...int64) (size int64, err error) {
	d.Lock()
	defer d.Unlock()

	//KIM: we can't get around having to perform the file i/o with size
	// since we need to do the comparison and return it no matter what
	if !d.configured {
		return -1, errors.New(ErrNotConfigured)
	}
	if size, err = internal.Sizes(d.config.Directory, d.config.FilePrefix, internal.FileExtensionIndex); err != nil {
		return
	}
	if len(indexSize) <= 0 || size > indexSize[0] {
		if err = d.readIndexes(false); err != nil {
			return
		}
	}

	return
}

//Write will sequentially write an element to the store and increment the
// internal index by one, it accepts a slice of bytes or one or more
// pointers/structs that implement the encoding.BinaryMarshaler interface
func (d *durostore) Write(i ...interface{}) (uint64, error) {
	d.Lock()
	defer d.Unlock()

	var writeFileIndex, writeFileData string
	var writeIndex = d.writeIndex
	var items []interface{}
	var data []internal.Bytes
	var err error

	//switch on the type of the element, attempt to get bytes in a supported
	// way, lock the directory for writing, and then write the data to the
	// appropriate file, increment the write index, update the write index
	// then update the in-memory index and then check the file size, if
	// greater than max size of the data file, increment the write files and
	// update the pointer
	//KIM: on write (as a rule) we don't store it in the map since we're
	// already offloading it to disk and don't want a functional memory leak
	// since data should be periodically read in chunks from disk by internal.ReadData()
	if !d.configured {
		return 0, errors.New(ErrNotConfigured)
	}
	if err := internal.CheckStoreSize(d.config.Directory, d.config.FilePrefix, d.config.MaxFiles, d.config.MaxFileSize); err != nil {
		return 0, err
	}
	if len(i) <= 0 {
		return 0, errors.New(ErrNoItemsToWrite)
	}
	for _, item := range i {
		switch v := item.(type) {
		default:
			return 0, errors.Errorf(ErrUnsupportedTypef, v)
		case encoding.BinaryMarshaler, []byte:
			items = append(items, v)
		case []interface{}:
			items = append(items, v...)
		}
	}
	d.fLock()
	defer d.fUnlock()
	for _, item := range items {
		switch w := item.(type) {
		case encoding.BinaryMarshaler:
			var bytes []byte

			if bytes, err = w.MarshalBinary(); err != nil {
				return 0, err
			}
			data = append(data, bytes)
		case []byte:
			data = append(data, w)
		}
	}
	for stop := false; !stop; {
		var starts, finishes []int64
		var indexes []*internal.Index

		if starts, finishes, data, err = internal.WriteData(d.files.DataWrite, d.config.MaxFileSize, data); err != nil {
			return 0, err
		}
		for i, start := range starts {
			index := &internal.Index{
				Index:     writeIndex,
				Start:     start,
				Finish:    finishes[i],
				DataFile:  d.files.DataWrite,
				IndexFile: d.files.IndexWrite,
			}
			indexes = append(indexes, index)
			d.indexes[index.Index] = index
			writeIndex++
			d.writeFileDataSize += (index.Finish - index.Start)
		}
		if len(indexes) > 0 {
			if err = internal.WriteIndex(indexes...); err != nil {
				return 0, err
			}
			if d.writeFileDataSize > int64(d.config.MaxFileSize) {
				if writeFileIndex, writeFileData, err = internal.IncrementWriteFile(d.config.Directory, d.config.FilePrefix, d.config.MaxFiles); err != nil {
					return 0, err
				}
				d.writeFileDataSize = 0
				d.files.IndexWrite = writeFileIndex
				d.files.DataWrite = writeFileData
			}
		} else {
			stop = true
		}
	}
	d.writeIndex = writeIndex
	//KIM: we have to use write index -1, to reference the last
	// index that was written, rather than the "next" to be written
	return writeIndex - 1, nil
}

//Read will randomly read a given index from the store and provide it
// as a slice of bytes
func (d *durostore) Read(indices ...uint64) ([]byte, error) {
	d.Lock()
	defer d.Unlock()

	var i uint64

	//verify that the index exists, and if so, read the index
	// from data, and if it's not present in data attempt to
	// read from disk
	if !d.configured {
		return nil, errors.New(ErrNotConfigured)
	}
	switch {
	default:
		//KIM: this will always read where the read index is
		// if no data is deleted, it'll read the same data
		i = d.readIndex
	case len(indices) == 1:
		i = indices[0]
	case len(indices) > 1:
		return nil, errors.New(ErrTooManyIndicesToRead)
	}
	index, ok := d.indexes[i]
	//KIM: there shouldn't be a situation where there's an index
	// that exists that's deleted, but just in case, this should
	// catch it rather than fail open
	if !ok || index == nil || index.Deleted {
		return nil, errors.Errorf(ErrIndexNotFoundf, i)
	}
	b, ok := d.data[i]
	if !ok {
		if len(d.data) <= 0 {
			if err := d.readData(); err != nil {
				return nil, err
			}
		}
		if b, ok = d.data[i]; !ok {
			if d.size+(index.Finish-index.Start) > d.config.MaxChunk {
				return nil, errors.New(ErrMaxChunkData)
			}
			d.fRLock()
			defer d.fUnlock()
			if err := internal.ReadData(d.data, index); err != nil {
				return nil, err
			}
			if b, ok = d.data[i]; !ok {
				return nil, errors.Errorf(ErrIndexNotFoundf, i)
			}
			d.size = d.size + int64(len(*b))
		}
	}
	bytes := make([]byte, len(*b))
	//KIM: we want to copy bytes so there's not a lingering
	// pointer that might not be garbage collected since
	// there's no guarantee it'll be deleted
	copy(bytes, *b)
	return bytes, nil
}

//Delete will remove the index (but not delete the actual data) of a given
// index if it exists
func (d *durostore) Delete(i ...uint64) (err error) {
	d.Lock()
	defer d.Unlock()

	if !d.configured {
		return errors.New(ErrNotConfigured)
	}
	switch {
	default:
		if _, ok := d.indexes[d.readIndex]; !ok {
			return errors.Errorf(ErrIndexNotFoundf, i)
		}
		i = []uint64{d.readIndex}
	case len(i) > 0:
		for _, i := range i {
			if _, ok := d.indexes[i]; !ok {
				return errors.Errorf(ErrIndexNotFoundf, i)
			}
		}
	}
	indexes := make([]*internal.Index, 0, len(i))
	for _, i := range i {
		//KIM: the reason this process is so...arduous is because
		// we want to avoid a situation where there's inconsistency
		// if the write operation fails
		index := *d.indexes[i]
		index.Deleted = true
		indexes = append(indexes, &index)
	}
	d.fLock()
	defer d.fUnlock()
	if err = internal.WriteIndex(indexes...); err != nil {
		return
	}
	for _, i := range i {
		//KIM: we only want to reduce the in-memory size
		// if the data is actually in memory
		if b, ok := d.data[i]; ok {
			d.size = d.size - int64(len(*b))
		}
		delete(d.indexes, i)
		delete(d.data, i)
	}
	d.readIndex = findReadIndex(d.indexes)
	if len(d.data) <= 0 || len(d.indexes) <= 0 {
		//If there's no data in memory, prune up until the read index
		// and read in new data
		if err = d.readData(); err != nil {
			return
		}
		readIndexFile := ""
		if index, ok := d.indexes[d.readIndex]; ok {
			readIndexFile = index.IndexFile
		}
		if err = internal.PruneDirectory(d.config.Directory, d.config.FilePrefix, readIndexFile); err != nil {
			return
		}
	}

	return
}

func (d *durostore) Length() (length int) {
	d.RLock()
	defer d.RUnlock()
	return len(d.indexes)
}

func (d *durostore) Size() (size int64) {
	d.RLock()
	defer d.RUnlock()
	return d.size
}
