package durostore

import (
	internal_file "github.com/antonio-alexander/go-durostore/internal/file"
)

const (
	ErrUnsupportedTypef     string = "unsupported type: %T"
	ErrIndexNotFoundf       string = "index: %d not found"
	ErrNotDirectory         string = "file: %s is not a directory"
	ErrNotConfigured        string = "durostore not configured"
	ErrUnableMakeDir        string = "unable to make directory"
	ErrNoItemsToWrite       string = "no items to write"
	ErrTooManyIndicesToRead string = "more than one index provided for read"
	ErrMaxChunkData         string = "max chunk in-memory data reached"
	ErrMaxStoreSize         string = internal_file.ErrMaxStoreSize
)

//Configuration describes the different options to configure an instance of durostore
type Configuration struct {
	Directory   string `json:"directory" yaml:"directory"`         //directory to store files in
	MaxFiles    int    `json:"max_files" yaml:"max_files"`         //maximum number of files to generate
	MaxFileSize int64  `json:"max_file_size" yaml:"max_file_size"` //max size of each data file
	MaxChunk    int64  `json:"max_chunk" yaml:"max_chunk"`         //max chunk of data to read into memory at once
	FilePrefix  string `json:"file_prefix" yaml:"file_prefix"`     //the prefix for files
	FileLocking bool   `json:"file_locking" yaml:"file_locking"`   //whether or not to use file locking
}

//Owner is a set of functions that should only be used
// by the caller of the NewDurostore function
type Owner interface {
	//Close can be used to set the internal pointers to nil and prepare the underlying pointer
	// for garbage cleanup, the API doesn't guarantee that it can be re-used
	Close() (err error)

	//Load can be used to configure and initialize the business logic for a store, it will return a
	// channel for errors since some of the enqueue interfaces can't communicate errors
	Load(config Configuration) (err error)

	//PruneDirectory can be used to completely delete all data on disk while maintaining all in-memory
	// data, this is useful in situations where there's data loss, a single option index can be provided
	// if none is provided -1 is assumed and all files are deleted
	PruneDirectory(indices ...uint64) (err error)

	//UpdateIndexes will attempt to read all available indexes and update the in-memory indexes
	UpdateIndexes(indexSize ...int64) (size int64, err error)
}

//Reader is a subset of the Durostore interface that allows the ability to
// read from the store
type Reader interface {
	//Read will randomly read given data, whether that index is a uint64
	Read(indices ...uint64) (bytes []byte, err error)
}

type Writer interface {
	//Write will sequentially write an item to the store and increment the
	// internal index by one, it accepts a slice of bytes or one or more
	// pointers/structs that implement the encoding.BinaryMarshaler interface
	Write(items ...interface{}) (index uint64, err error)

	//Delete will remove the index (but not delete the actual data) of a given
	// index if it exists
	Delete(indices ...uint64) (err error)
}

type Info interface {
	//Length will return the total number of items available in the store
	Length() (size int)

	//Size will return the total size (in bytes)
	Size() (size int64)
}
