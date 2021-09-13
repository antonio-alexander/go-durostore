package queue_test

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	durostore "github.com/antonio-alexander/go-durostore"
	duroqueue "github.com/antonio-alexander/go-durostore/queue"
	goqueue "github.com/antonio-alexander/go-queue"

	store_example "github.com/antonio-alexander/go-durostore/internal/example"
	queue_example "github.com/antonio-alexander/go-durostore/queue/internal/example"

	goqueue_tests "github.com/antonio-alexander/go-queue/tests"

	"github.com/stretchr/testify/assert"
)

const (
	storeDirectory string = "./temp"
	filePrefix     string = "test"
)

var (
	pwd           string
	configuration durostore.Configuration
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
	pwd, _ = os.Getwd()
	configuration = durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    10,
		MaxFileSize: durostore.MegaByteToByte(10),
		MaxChunk:    durostore.MegaByteToByte(50),
		FilePrefix:  filePrefix,
	}
}

func new(size int) interface {
	goqueue.Dequeuer
	duroqueue.Duroqueue
	goqueue.Enqueuer
	goqueue.Event
	goqueue.Info
	goqueue.Owner
	goqueue.Peeker
} {
	store := durostore.New(configuration)
	if err := store.PruneDirectory(); err != nil {
		panic(err)
	}
	return duroqueue.New(store, size)
}

func TestNew(t *testing.T) {
	goqueue_tests.New(t, func(size int) interface {
		goqueue.Owner
		goqueue.Info
	} {
		return new(size)
	})
}

func TestDequeue(t *testing.T) {
	goqueue_tests.Dequeue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Info
	} {
		return new(size)
	})
}

func TestDequeueMultiple(t *testing.T) {
	goqueue_tests.DequeueMultiple(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return new(size)
	})
}

func TestFlush(t *testing.T) {
	goqueue_tests.Flush(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return new(size)
	})
}

func TestInfo(t *testing.T) {
	goqueue_tests.Info(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Info
	} {
		return new(size)
	})
}

func TestQueue(t *testing.T) {
	goqueue_tests.Queue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Info
	} {
		return new(size)
	})
}

func TestAsync(t *testing.T) {
	goqueue_tests.Async(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Info
	} {
		return new(size)
	})
}

func TestEvent(t *testing.T) {
	//REVIEW: this test is flakey, possibly due to timeout??
	goqueue_tests.Event(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Event
	} {
		return new(size)
	})
}

//REVIEW: this doesn't work because the enqueue function doesn't
// overflow as expected when the queue is full
// func TestEnqueue(t *testing.T) {
// 	finite_tests.Enqueue(t, func(size int) interface {
// 		goqueue.Owner
// 		goqueue.Enqueuer
// 	} {
// 		return new(size)
// 	})
// }

//REVIEW: this doesn't work because enqueue multiple isn't
// implemented the same??
// func TestEnqueueMultiple(t *testing.T) {
// 	finite_tests.EnqueueMultiple(t, func(size int) interface {
// 		goqueue.Owner
// 		goqueue.Enqueuer
// 	} {
// 		return new(size)
// 	})
// }

//REVIEW: we need to write custom functions to confirm
// the peek functionality since we only really implement
// PeekHead()

// func TestPeek(t *testing.T) {
// 	goqueue_tests.Peek(t, func(size int) interface {
// 		goqueue.Owner
// 		goqueue.Enqueuer
// 		goqueue.Dequeuer
// 		goqueue.Info
// 		goqueue.Peeker
// 	} {
// 		return new(size)
// 	})
// }

// func TestPeekFromHead(t *testing.T) {
// 	goqueue_tests.PeekFromHead(t, func(size int) interface {
// 		goqueue.Owner
// 		goqueue.Enqueuer
// 		goqueue.Dequeuer
// 		goqueue.Info
// 		goqueue.Peeker
// 	} {
// 		return new(size)
// 	})
// }

func TestEnqueueDequeue(t *testing.T) {
	store := durostore.New(configuration)
	err := store.PruneDirectory()
	assert.Nil(t, err)
	queue := duroqueue.New(store, 1)
	for i := 0; i < 10; i++ {
		overflow := queue.Enqueue(&store_example.Data{Int: i})
		assert.False(t, overflow)
	}
	assert.Equal(t, 10, queue.Length())
	for i := 0; i < 10; i++ {
		data, err := queue_example.Dequeue(queue)
		assert.Nil(t, err)
		if assert.NotNil(t, data) {
			assert.Equal(t, i, data.Int)
		}
	}
	queue.Close()
	err = store.Close()
	assert.Nil(t, err)
}

func TestProducerConsumer(t *testing.T) {
	var wg sync.WaitGroup
	var err error

	queueSize := 1
	config := durostore.Configuration{
		Directory:   storeDirectory,
		MaxFiles:    10,
		MaxFileSize: durostore.MegaByteToByte(10),
		MaxChunk:    durostore.MegaByteToByte(1),
		FilePrefix:  filePrefix,
		FileLocking: false,
	}
	storeProducer := durostore.New(config)
	err = storeProducer.PruneDirectory()
	assert.Nil(t, err)
	queueProducer := duroqueue.New(storeProducer, queueSize)
	storeConsumer := durostore.New(config)
	queueConsumer := duroqueue.New(storeConsumer, queueSize)
	stopper := make(chan struct{})
	started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		size, err := storeConsumer.UpdateIndexes()
		assert.Nil(t, err)
		tCheck := time.NewTicker(time.Second)
		defer tCheck.Stop()
		close(started)
		for {
			select {
			case <-tCheck.C:
				size, err = storeConsumer.UpdateIndexes(size)
				assert.Nil(t, err)
			case <-stopper:
				return
			}
		}
	}()
	<-started
	for i := 0; i < 10; i++ {
		overflow := queueProducer.Enqueue(&store_example.Data{Int: i})
		assert.False(t, overflow)
	}
	queueProducer.Close()
	err = storeProducer.Close()
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)
	for i := 0; i < 10; i++ {
		data, err := queue_example.Dequeue(queueConsumer)
		assert.Nil(t, err)
		if assert.NotNil(t, data) {
			assert.Equal(t, i, data.Int)
		}
	}
	close(stopper)
	wg.Wait()
	queueConsumer.Close()
	err = storeConsumer.Close()
	assert.Nil(t, err)
}
