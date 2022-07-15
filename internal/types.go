package file

import (
	"encoding/binary"

	errors "github.com/pkg/errors"
)

//error constants
const (
	ErrLessBytes       string = "not enough bytes to decode"
	ErrBadBytesWritten string = "bytes written not equal to input slice"
	ErrMaxFilesCreated string = "max files created, store is full"
	ErrMaxStoreSize    string = "max store size reached, store is full"
)

const (
	SizeIndex          int    = 25
	FileIndexf         string = "%s_%03d.idx"
	FileDataf          string = "%s_%03d.dat"
	FileLockf          string = "%s_lock.lock"
	ScanFilef          string = "%s_%s%s"
	FileExtensionIndex string = ".idx"
	FileExtensionData  string = ".dat"
	RecoverPanicf      string = "recovered from panic: %v"
)

//Bytes is used to simplify some internal operations
type Bytes []byte

//Files
type Files struct {
	LockFile   string
	IndexWrite string
	DataRead   string
	DataWrite  string
}

//Index is a type used to describe a fixed-size index of
// a single data entry of a store, although all fields are
// exported, only Index, Start, Finish and Deleted are actually
// stored on disk, DataFile, IndexFile, and MinIndex are populated
// when the index file is read in as a whole
type Index struct {
	DataFile  string //the data file associated with the index
	IndexFile string //the index file associated with the index
	MinIndex  uint64 //minimum index for the file
	Index     uint64 //the index/key
	Start     int64  //the file start offset
	Finish    int64  //the file finish offset
	Deleted   bool   //whether or not the index is deleted
}

//MarshalBinary will convert the provided index into
// a byte slice
func (i *Index) MarshalBinary() (data []byte, err error) {
	//create output slice the size of size index and
	// encode the provided index
	data = make([]byte, SizeIndex)
	if i.Deleted {
		data[0] = 1
	}
	binary.BigEndian.PutUint64(data[1:], i.Index)
	binary.BigEndian.PutUint64(data[9:], uint64(i.Start))
	binary.BigEndian.PutUint64(data[17:], uint64(i.Finish))

	return
}

//UnmarshalBinary will decode a slice of bytes into an index
func (i *Index) UnmarshalBinary(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf(RecoverPanicf, r)
		}
	}()

	//defer a recover function in the case of an unexpected
	// panic, check the length of the size index and if
	// less than return, otherwise attempt to decode the index
	if len(data) < SizeIndex {
		err = errors.New(ErrLessBytes)

		return
	}
	i.Deleted = data[0] != 0
	i.Index = binary.BigEndian.Uint64(data[1:])
	i.Start = int64(binary.BigEndian.Uint64(data[9:]))
	i.Finish = int64(binary.BigEndian.Uint64(data[17:]))

	return
}
