package durostore_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	durostore "github.com/antonio-alexander/go-durostore"

	store_example "github.com/antonio-alexander/go-durostore/internal/example"

	"github.com/stretchr/testify/assert"
)

const (
	storeDirectory string = "./temp"
	filePrefix     string = "test"
)

var configuration durostore.Configuration

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
	configuration = durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    10,
		MaxFileSize: durostore.MegaByteToByte(10),
		MaxChunk:    durostore.MegaByteToByte(50),
		FilePrefix:  filePrefix,
	}
}

func TestRead(t *testing.T) {
	//This test attempts to confirm the functional of the Read() function:
	// (1) Do we get an error when attempting to read data that doesn't exist
	// (2) Can we read data that exists?
	// (3) Can we read data that has been deleted?
	// (4) Create a database entry

	d := durostore.New()
	_, err := d.Read() //attempt to read before configuring the store
	assert.NotNil(t, err)
	err = d.Load(configuration)
	assert.Nil(t, err)
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err = d.PruneDirectory()
	assert.Nil(t, err)
	_, err = d.Read(0) //attempt to read data that doesn't exist
	assert.NotNil(t, err)
	example1 := &store_example.Data{}
	index1, err := d.Write(example1)
	assert.Nil(t, err)
	assert.Equal(t, index1, uint64(0))
	bytes, err := d.Read(index1) //attempt to read data that exists
	assert.Nil(t, err)
	exampleData := &store_example.Data{}
	err = exampleData.UnmarshalBinary(bytes)
	assert.Nil(t, err)
	assert.Equal(t, exampleData, example1)
	d.Close()
	d = nil
	d = durostore.New(configuration)
	bytes, err = d.Read(index1) //attempt to read data that exists in new store
	assert.Nil(t, err)
	err = exampleData.UnmarshalBinary(bytes)
	assert.Nil(t, err)
	assert.Equal(t, exampleData, example1)
	err = d.Delete(index1)
	assert.Nil(t, err)
	_, err = d.Read(index1) //attempt to read data that's been deleted
	assert.NotNil(t, err)
}

func TestWrite(t *testing.T) {
	//This test will confirm functionality of the Write() function:
	// (1) Attempt to write single element (MarshalBinary, bytes)
	// (2) Attempt to write multiple elements (MarshalBinary, bytes)
	// (3) Attempt to write nil
	// (4) Attempt to write data that doesn't implement marshalbinary

	d := durostore.New()
	_, err := d.Write([]byte{}) //attempt to write before configuring store
	assert.NotNil(t, err)
	err = d.Load(configuration)
	assert.Nil(t, err)
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err = d.PruneDirectory()
	assert.Nil(t, err)
	//attempt to write single element (MarshalBinary, bytes)
	//TODO: attempt to write []bytes
	exampleData := &store_example.Data{
		Int:    rand.Int(),
		String: fmt.Sprint(rand.Int()),
	}
	index, err := d.Write(exampleData)
	assert.Nil(t, err)
	assert.Equal(t, exampleData, store_example.Read(d, index))
	oldIndex := index
	// (2) Attempt to write multiple elements (MarshalBinary, bytes)
	//TODO: attempt to write []bytes
	exampleDatas := []interface{}{
		&store_example.Data{
			Int:    rand.Int(),
			String: fmt.Sprint(rand.Int()),
		},
		&store_example.Data{
			Int:    rand.Int(),
			String: fmt.Sprint(rand.Int()),
		},
	}
	index, err = d.Write(exampleDatas)
	assert.Nil(t, err)
	assert.Greater(t, index, oldIndex)
	assert.Condition(t, func() (success bool) {
		for i, data := range exampleDatas {
			if !reflect.DeepEqual(data, store_example.Read(d, uint64(i)+1)) {
				return false
			}
		}
		return true
	})
	// (3) Attempt to write nil
	index, err = d.Write(nil)
	assert.NotNil(t, err)
	assert.Zero(t, index)
	// (4) Attempt to write data that doesn't implement marshalbinary
	index, err = d.Write(new(int))
	assert.NotNil(t, err)
	assert.Zero(t, index)
	d.Close()
}

func TestDelete(t *testing.T) {
	//This test will confirm functionality of the Delete() function:
	// (1) Attempt to delete valid data
	// (2) Attempt to delete valid data twice
	// (3) Attempt to delete data that doesn't exist

	d := durostore.New()
	err := d.Delete(0) //attempt to delete before configuring store
	assert.NotNil(t, err)
	err = d.Load(configuration)
	assert.Nil(t, err)
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err = d.PruneDirectory()
	assert.Nil(t, err)
	exampleData := &store_example.Data{
		Int:    rand.Int(),
		String: fmt.Sprint(rand.Int()),
	}
	index, err := d.Write(exampleData)
	assert.Nil(t, err)
	err = d.Delete() //delete valid data without index
	assert.Nil(t, err)
	err = d.Delete(index) //delete valid data twice
	assert.NotNil(t, err)
	err = d.Delete(1) //delete data that doesn't exist
	assert.NotNil(t, err)
	exampleData = &store_example.Data{
		Int:    rand.Int(),
		String: fmt.Sprint(rand.Int()),
	}
	index, err = d.Write(exampleData)
	assert.Nil(t, err)
	d.Close()
	d = nil
	d = durostore.New(configuration)
	err = d.Delete(index) //delete valid data
	assert.Nil(t, err)
	err = d.Delete(index) //delete valid data twice
	assert.NotNil(t, err)
	d.Close()
}

func TestLoad(t *testing.T) {
	//This test will confirm functionality of the Load() function:
	// (1) attempt to load via the new function
	// (2) attempt to load via the Load function
	// (3) attempt to load with file

	c := configuration
	d := durostore.New(c) //load via new
	err := d.Load(c)      //load via load
	assert.Nil(t, err)
	c.Directory = filepath.Join(storeDirectory, ".gitkeep")
	err = d.Load(c) //load with file
	assert.NotNil(t, err)
	d.Close()
	func() {
		defer func() {
			r := recover()
			assert.NotNil(t, r)
		}()
		c.Directory = filepath.Join(storeDirectory, ".gitkeep")
		durostore.New(c)
		assert.Fail(t, "expected panic didn't occur")
	}()
}

func TestUpdateIndexes(t *testing.T) {
	maxSize := durostore.MegaByteToByte(1)
	d := durostore.New()
	_, err := d.UpdateIndexes()
	assert.NotNil(t, err)
	err = d.Load(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    2,
		MaxFileSize: maxSize,
		MaxChunk:    -1, //disable the chunking
		FilePrefix:  filePrefix,
	})
	assert.Nil(t, err)
	err = d.PruneDirectory()
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err := d.Write(&store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		})
		assert.Nil(t, err)
	}
	_, err = d.UpdateIndexes()
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		dataExpected := &store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		}
		data := store_example.Read(d)
		assert.Equal(t, dataExpected, data)
		err = d.Delete()
		assert.Nil(t, err)
	}
	err = d.PruneDirectory()
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err := d.Write(&store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		})
		assert.Nil(t, err)
	}
	_, err = d.UpdateIndexes(0)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		dataExpected := &store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		}
		data := store_example.Read(d)
		assert.Equal(t, dataExpected, data)
		err = d.Delete()
		assert.Nil(t, err)
	}
	err = d.Close()
	assert.Nil(t, err)
}

func TestMaxStoreSize(t *testing.T) {
	//
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	maxSize := durostore.MegaByteToByte(1)
	d := durostore.New(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    2,
		MaxFileSize: maxSize,
		MaxChunk:    -1, //disable the chunking
		FilePrefix:  filePrefix,
	})
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err := d.PruneDirectory()
	assert.Nil(t, err)
	bytes := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		bytes = append(bytes, byte(rand.Int()))
	}
	//write a byte until there's an error
	for _, err = d.Write(bytes); err == nil; {
		_, err = d.Write(bytes)
	}
	if assert.NotNil(t, err) {
		assert.Equal(t, durostore.ErrMaxStoreSize, err.Error())
	}
	d.Close()
}

func TestMaxChunkSize(t *testing.T) {
	maxSize := durostore.MegaByteToByte(10)
	d := durostore.New(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    2,
		MaxFileSize: maxSize,
		// MaxChunk:    -1, //this disables the chunking
		FilePrefix: filePrefix,
	})
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err := d.PruneDirectory()
	assert.Nil(t, err)
	bytes := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		bytes = append(bytes, byte(rand.Int()))
	}
	//write a kilobyte until there's an error
	i, index := uint64(0), uint64(0)
	for err = nil; err == nil; {
		if i, err = d.Write(bytes); err == nil {
			index = i
		}
	}
	//load to get everything into memory
	maxChunk := durostore.MegaByteToByte(10)
	err = d.Load(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    2,
		MaxFileSize: maxSize,
		MaxChunk:    maxChunk,
		FilePrefix:  filePrefix,
	})
	assert.Nil(t, err)
	//attempt to read the last index written
	_, err = d.Read(index)
	if assert.NotNil(t, err) {
		assert.Equal(t, durostore.ErrMaxChunkData, err.Error())
	}
	assert.GreaterOrEqual(t, d.Size(), maxChunk)
	d.Close()
}

func TestAsyncWrite(t *testing.T) {
	var exampleDatas []*store_example.Data
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		exampleDatas = append(exampleDatas, &store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		})
	}
	d := durostore.New(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    10,
		MaxFileSize: durostore.MegaByteToByte(10),
		MaxChunk:    durostore.MegaByteToByte(50),
		FilePrefix:  filePrefix,
		FileLocking: true,
	})
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err := d.PruneDirectory()
	assert.Nil(t, err)
	chExample := make(chan *store_example.Data)
	wg.Add(3)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for data := range chExample {
				_, err := d.Write(data)
				assert.Nil(t, err)
			}
		}()
	}
	go func() {
		defer wg.Done()

		for _, data := range exampleDatas {
			chExample <- data
		}
		close(chExample)
	}()
	wg.Wait()
	size := d.Length()
	assert.Equal(t, len(exampleDatas), size)
	assert.Condition(t, func() bool {
		indexes := make(map[uint64]struct{})
		for i := 0; i < d.Length(); i++ {
			indexes[uint64(i)] = struct{}{}
		}
		e := &store_example.Data{}
		for _, data := range exampleDatas {
			for index := range indexes {
				bytes, err := d.Read(index)
				assert.Nil(t, err)
				err = e.UnmarshalBinary(bytes)
				assert.Nil(t, err)
				if reflect.DeepEqual(data, e) {
					delete(indexes, index)
				}
			}
		}
		return len(indexes) <= 0
	})
	d.Close()
}

func TestAsyncRead(t *testing.T) {
	var exampleDatas []*store_example.Data
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		exampleDatas = append(exampleDatas, &store_example.Data{
			Int:    i,
			String: fmt.Sprint(i),
		})
	}
	d := durostore.New(durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    10,
		MaxFileSize: durostore.MegaByteToByte(10),
		MaxChunk:    durostore.MegaByteToByte(50),
		FilePrefix:  filePrefix,
		FileLocking: true,
	})
	//KIM: this will ensure that we start from a clean directory/situation
	// for durostore
	err := d.PruneDirectory()
	assert.Nil(t, err)
	for _, data := range exampleDatas {
		_, err := d.Write(data)
		assert.Nil(t, err)
	}
	wg.Add(2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			<-start
			e := &store_example.Data{}
			for i := 0; i < d.Length(); i++ {
				bytes, err := d.Read(uint64(i))
				assert.Nil(t, err)
				err = e.UnmarshalBinary(bytes)
				assert.Nil(t, err)
				assert.Equal(t, exampleDatas[i], e)
			}
		}()
	}
	close(start)
	wg.Wait()
	d.Close()
}

//REVIEW: things we can't test easily
// * errors with flock
// * file.ReadIndexes() error
// * file.ReadData() error
//

//TODO: prune when not configured
//TODO: read with no index
//TODO: read where index doesn't exist in the file??
//TODO: delete with no indices
//TODO: ReadIndexes()
//TODO: durostore with a directory that doesn't exist?
