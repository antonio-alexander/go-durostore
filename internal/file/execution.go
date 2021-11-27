package file

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

//findFiles will attempt to find the min and max indexes present in the files that aren't deleted and provide the appropriate read/write index
// and data files
func FindFiles(directory, filePrefix string) (files Files, dataSizeWrite int64, err error) {
	var maxIndex, minIndex uint64
	var maxDataSize int64

	if minIndex, maxIndex, maxDataSize, _, err = FindFileIndexes(directory, filePrefix); err != nil {
		return
	}
	dataSizeWrite = maxDataSize
	files.LockFile = filepath.Join(directory, fmt.Sprintf(FileLockf, filePrefix))
	files.IndexWrite = filepath.Join(directory, fmt.Sprintf(FileIndexf, filePrefix, maxIndex))
	files.DataRead = filepath.Join(directory, fmt.Sprintf(FileDataf, filePrefix, minIndex))
	files.DataWrite = filepath.Join(directory, fmt.Sprintf(FileDataf, filePrefix, maxIndex))

	return
}

//CheckStoreSize can be used to determine if the store is full
func CheckStoreSize(directory, filePrefix string, maxFiles int, maxFileSize int64) (err error) {
	var files []os.FileInfo
	var totalSize int64

	//read the files in the directory, range over each of the files
	// ensuring that we only work with the data files and calculate
	// the total size of the store, finally check the size
	// and if its full, output an error
	if maxFiles <= 0 {
		maxFiles = 1
	}
	if files, err = ioutil.ReadDir(directory); err != nil {
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), FileExtensionData) && strings.HasPrefix(f.Name(), filePrefix) {
			totalSize = totalSize + f.Size()
		}
	}
	if totalSize >= maxFileSize*int64(maxFiles) {
		err = errors.New(ErrMaxStoreSize)

		return
	}

	return
}

func FindFileIndexes(directory, filePrefix string) (minIndex, maxIndex uint64, maxDataSize int64, totalFiles int, err error) {
	var files []os.FileInfo
	var dataSizes []int64
	var indexes []uint64
	var iMax, n int
	var info os.FileInfo
	var file *os.File
	var index uint64

	if files, err = ioutil.ReadDir(directory); err != nil {
		return
	}
	if len(files) > 0 {
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), FileExtensionIndex) && strings.HasPrefix(f.Name(), filePrefix) {
				if n, err = fmt.Sscanf(f.Name(), fmt.Sprintf(ScanFilef, filePrefix, "%d", FileExtensionIndex), &index); err != nil {
					return
				}
				if n > 0 {
					totalFiles++
					if file, err = os.Open(filepath.Join(directory, fmt.Sprintf(FileDataf, filePrefix, index))); err != nil {
						file.Close()

						return
					}
					if info, err = file.Stat(); err != nil {
						file.Close()

						return
					}
					if err = file.Close(); err != nil {
						return
					}
					indexes = append(indexes, index)
					dataSizes = append(dataSizes, info.Size())
				}
			}
		}
		if totalFiles > 0 {
			maxIndex = 0
			for i, index := range indexes {
				if index > maxIndex {
					maxIndex = index
					iMax = i
				}
			}
			minIndex = maxIndex
			for _, index := range indexes {
				if index < minIndex {
					minIndex = index
				}
			}
			maxDataSize = dataSizes[iMax]
		} else {
			minIndex, maxIndex = 1, 1
		}
	} else {
		minIndex, maxIndex = 1, 1
	}

	return
}

//incrementWriteFile can be used to increment the index and data files associated with a store
// it will pull the current index and generate new index/data filepaths
func IncrementWriteFile(directory, filePrefix string, maxFiles int) (index, data string, err error) {
	var maxIndex uint64
	var totalFiles int

	//get the max index (which would be the file to write to), increment
	// it by one and generate the index and data files
	if _, maxIndex, _, totalFiles, err = FindFileIndexes(directory, filePrefix); err != nil {
		return
	}
	if totalFiles >= maxFiles {
		err = errors.New(ErrMaxFilesCreated)

		return
	}
	maxIndex++
	index = filepath.Join(directory, fmt.Sprintf(FileIndexf, filePrefix, maxIndex))
	data = filepath.Join(directory, fmt.Sprintf(FileDataf, filePrefix, maxIndex))

	return
}

//pruneDirectory can be used to remove un-used files less than the provided
// index or all files with the given file prefix if set to -1
func PruneDirectory(directory, filePrefix, readIndexFile string) (err error) {
	var readFileIndex = int64(-1)
	var files []os.FileInfo
	var index int64
	var n int

	if readIndexFile != "" {
		if _, err = fmt.Sscanf(filepath.Base(readIndexFile), fmt.Sprintf(ScanFilef, filePrefix, "%d", FileExtensionIndex), &index); err != nil {
			return
		}
		readFileIndex = int64(n)
	}
	//read all the files in the directory, if the files end in idx or dat
	// remove them if they're not found in the indexes
	if files, err = ioutil.ReadDir(directory); err != nil {
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), filePrefix) {
			switch {
			case strings.HasSuffix(f.Name(), FileExtensionIndex):
				if n, err = fmt.Sscanf(f.Name(), fmt.Sprintf(ScanFilef, filePrefix, "%d", FileExtensionIndex), &index); err != nil {
					return
				}
			case strings.HasSuffix(f.Name(), FileExtensionData):
				if n, err = fmt.Sscanf(f.Name(), fmt.Sprintf(ScanFilef, filePrefix, "%d", FileExtensionData), &index); err != nil {
					return
				}
			}
			if n > 0 {
				switch readFileIndex {
				case -1: //delete each file that matches
					if err = os.Remove(filepath.Join(directory, f.Name())); err != nil {
						return
					}
				default: //only delete items that have been read
					if index < readFileIndex {
						if err = os.Remove(filepath.Join(directory, f.Name())); err != nil {
							return
						}
					}
				}
			}
		}
	}

	return
}

//writeData can be used to write one or more slices of data to disk
func WriteData(dataFile string, maxFileSize int64, dataIn []Bytes) (starts, finishes []int64, dataOut []Bytes, err error) {
	var info os.FileInfo
	var file *os.File
	var bytes []byte
	var idx int64
	var n int

	//ensure that there is data to write, open the file, get its size, and then
	// determine all the data to write up until the max file size, populate dataOut
	// with the remaining Bytes and then write all the bytes to disk at once
	if len(dataIn) <= 0 {
		return
	}
	if file, err = os.OpenFile(dataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600); err != nil {
		file.Close()
		return
	}
	defer file.Close()
	if info, err = file.Stat(); err != nil {
		return
	}
	idx = info.Size()
	for i, d := range dataIn {
		starts = append(starts, idx)
		idx += int64(len(d))
		finishes = append(finishes, idx)
		bytes = append(bytes, d...)
		if idx > maxFileSize && i < len(d) {
			dataOut = dataIn[i+1:]
			break
		}
	}
	if n, err = file.Write(bytes); err != nil {
		return
	} else if n != len(bytes) {
		err = errors.New(ErrBadBytesWritten)

		return
	}

	return
}

//readData can be used to read one or more indexes
func ReadData(data map[uint64]*[]byte, indexes ...*Index) (err error) {
	var dataFile string
	var file *os.File

	//range through the indexes, attempting to keep a file open as long as the file
	// name doesn't change, seek through the file and read the bytes, assigning to
	// the output data map organizing by index
	defer func() {
		if file != nil {
			if err != nil {
				file.Close()
			} else {
				err = file.Close()
			}
		}
	}()
	for _, i := range indexes {
		var bytes []byte

		if dataFile != i.DataFile {
			if dataFile != "" {
				if err = file.Close(); err != nil {
					return
				}
			}
			dataFile = i.DataFile
			if file, err = os.OpenFile(dataFile, os.O_RDONLY, 0700); err != nil {
				return
			}
		}
		if _, err = file.Seek(i.Start, 0); err != nil {
			return
		}
		bytes = make([]byte, i.Finish-i.Start)
		if _, err = file.Read(bytes); err != nil {
			return
		}
		data[i.Index] = &bytes
	}

	return
}

//writeIndex can be used to write multiple indexes
func WriteIndex(indexes ...*Index) (err error) {
	var fileSize, idx int64
	var append, edit bool
	var info os.FileInfo
	var filename string
	var file *os.File
	var bytes []byte
	var n int

	//ensure that there are indexes to write, range over each index
	// and write to the index file, trying to keep the file open
	// for as long as possible without closing it to optimize the
	// process
	if len(indexes) <= 0 {
		return
	}
	for _, i := range indexes {
		if i.IndexFile != filename {
			file.Close()
			if file, err = os.Open(i.IndexFile); err != nil {
				file.Close()
				err = nil
			} else {
				if info, err = file.Stat(); err != nil {
					file.Close()

					return
				}
				fileSize = info.Size()
				if err = file.Close(); err != nil {
					return
				}
			}
			filename = i.IndexFile
		}
		idx = int64((i.Index - i.MinIndex)) * int64(SizeIndex)
		if bytes, err = i.MarshalBinary(); err != nil {
			return
		}
		if idx < fileSize && fileSize != 0 {
			if !edit {
				file.Close()
				if file, err = os.OpenFile(i.IndexFile, os.O_WRONLY, 0600); err != nil {
					file.Close()

					return
				}
			}
			if _, err = file.WriteAt(bytes, idx); err != nil {
				file.Close()

				return
			}
			edit = true
		} else {
			if !append {
				file.Close()
				if file, err = os.OpenFile(i.IndexFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600); err != nil {
					file.Close()

					return
				}
			}
			if n, err = file.Write(bytes); err != nil {
				return
			}
			fileSize += int64(n)
			append = true
		}
	}
	err = file.Close()

	return
}

func ReadIndex(indexFile string) (indexes []*Index, indexRead, indexWrite uint64, err error) {
	var indexMin uint64
	var bytes []byte
	var idx int

	//read the bytes from the file and then range over those bytes to read
	// an index at a time, store those indexes, and then find the
	// minimum index (stored internally to find the pointer in a file)
	// and then the maximum index (this is where new indexes are written)
	// find the read index by ranging over the indexes and choosing the
	// lowest index where the index isn't deleted, we could further optimize
	// by not outputing those deleted indexes, but it's pruned in the above
	// step when the map is created (it's easier to do there than here)
	if bytes, err = ioutil.ReadFile(indexFile); err != nil {
		return
	}
	if len(bytes) >= SizeIndex {
		for stop := false; !stop; {
			if len(bytes[idx:]) >= SizeIndex {
				var index = Index{
					IndexFile: indexFile,
					DataFile:  fmt.Sprintf("%s%s", strings.TrimSuffix(indexFile, FileExtensionIndex), FileExtensionData),
				}

				if err = index.UnmarshalBinary(bytes[idx:]); err != nil {
					return
				}
				indexes = append(indexes, &index)
				idx += SizeIndex
			} else {
				stop = true
			}
		}
	}
	indexWrite = 0
	for _, i := range indexes {
		if i.Index > indexWrite {
			indexWrite = i.Index
		}
	}
	indexMin = indexWrite
	for _, i := range indexes {
		if i.Index < indexMin {
			indexMin = i.Index
		}
	}
	for i, index := range indexes {
		index.MinIndex = indexMin
		indexes[i] = index
	}
	indexRead = indexWrite
	for _, i := range indexes {
		if i.Index < indexRead && !i.Deleted {
			indexRead = i.Index
		}
	}

	return
}

//ReadIndexes will take a directory, get all the index files from it and load them into an indices map
// while ignoring any deleted indexes
func ReadIndexes(directory, filePrefix string, indices map[uint64]*Index) (indexRead, indexWrite uint64, err error) {
	var indexReads, indexWrites []uint64
	var files []os.FileInfo

	//populate the indices map, read the directory while filtering for index files that
	// begin with the provided prefix, read the indexes of each of those files and
	// populate the indices map, taking care to filter the deleted indexes
	if files, err = ioutil.ReadDir(directory); err != nil {
		return
	}
	if len(files) > 0 {
		for _, f := range files {
			if !f.IsDir() && strings.HasPrefix(f.Name(), filePrefix) && strings.HasSuffix(f.Name(), FileExtensionIndex) {
				var iRead, iWrite uint64
				var indexes []*Index

				if indexes, iRead, iWrite, err = ReadIndex(filepath.Join(directory, f.Name())); err != nil {
					return
				}
				indexReads = append(indexReads, iRead)
				indexWrites = append(indexWrites, iWrite)
				for _, i := range indexes {
					if !i.Deleted {
						indices[i.Index] = i
					}
				}
			}
		}
		indexWrite = 0
		for _, i := range indexWrites {
			if i > indexWrite {
				indexWrite = i
			}
		}
		indexRead = indexWrite
		for _, i := range indexReads {
			if i < indexRead {
				indexRead = i
			}
		}
	}

	return
}

func Size(filepath string) (size int64, err error) {
	var fileInfo os.FileInfo
	var file *os.File

	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()
	if fileInfo, err = file.Stat(); err != nil {
		return
	}
	size = fileInfo.Size()

	return
}

func Sizes(directory, filePrefix, fileSuffix string) (int64, error) {
	var files []os.FileInfo
	var size int64
	var err error

	//populate the indices map, read the directory while filtering for index files that
	// begin with the provided prefix, read the indexes of each of those files and
	// populate the indices map, taking care to filter the deleted indexes
	if files, err = ioutil.ReadDir(directory); err != nil {
		return 0, err
	}
	if len(files) > 0 {
		for _, f := range files {
			if !f.IsDir() && strings.HasPrefix(f.Name(), filePrefix) && strings.HasSuffix(f.Name(), fileSuffix) {
				//calculate the size
				if s, err := Size(filepath.Join(directory, f.Name())); err != nil {
					return 0, err
				} else {
					size += s
				}
			}
		}
	}
	return size, nil
}

func MegaByteToByte(n int64) int64 {
	return n * 1024 * 1024
}
