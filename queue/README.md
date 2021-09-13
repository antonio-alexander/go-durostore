# go-durostore/queue (github.com/antonio-alexander/go-durostore/queue)

duroqueue provides a finite implementation of a queue/fifo on top of durostore. Durostore's main weakness is that under enough load it'll constantly require file i/o to read/write values. Depending on how much data you push through or what you actually need persisted, this may be wholly inefficient.

For more information about queues, look at: [http://github.com/antonio-alexander/go-queue](http://github.com/antonio-alexander/go-queue)

Keep the following in mind:

- queue has no first-hand knowledge of what happens with the durostore instance provided on instantiation, be careful when performing destructive operations when the queue is in-use
- it's impossible to maintain the error handling of durostore when wrapped with the queue interface

## Error handling

Because the queue interfaces aren't designed to output errors AND there's some concurrent processing that can occur, error handling is provided via an Error Handler that implements this function type:

```go
type ErrorHandler func(err error)
```

This is a simple error handler that simply prints the error, this works, but may obviously be a little useless in practice.

```go
func SimpleErrorHandler(err error) {
    fmt.Println(err)
}
```

This is a slightly more complex error handler were you inject a channel to output the error to be read by some business logic. This takes into the possibility that the channel isn't avaialble by pairing it with a timeout.

```go
func ComplexErrorHandler(ch chan error, t time.Duration) duroqueue.ErrorHandler {
    timeout := time.Millisecond
    if timeout > 0 {
        timeout = t
    }
    return func(err error) {
        select {
        case ch <- err:
        case <-time.After(timeout):
        }
    }
}
```

The error handler will always be called behind a mutex owned by duroqueue and since it's in the critical path, be careful NOT to create situations where it blocks for an extended time

## Shared queue interfaces

Although this implements the following interfaces of [github.com/antonio-alexander/go-queue](http://github.com/antonio-alexander/go-queue), they are slightly modified to lean toward the needs of durostore. These interfaces exist here: [http://github.com/antonio-alexander/go-queue/blob/main/types.go](http://github.com/antonio-alexander/go-queue/blob/main/types.go).

At a very high-level, as long as you don't exceed the size of the queue, all of the interfaces are implemented like a finite queue. When you do exceed the size of the queue, it'll "overflow" into the store (and cause file i/o).

The Close() function is implemented to return any items that remain in the queue, but it will ALSO flush all items from the queue into the store upon close (so no data is lost). In practice there's no strong use for the remaining items (feel free to call close and omit the returned items).

```go
type Owner interface {
    Close() (items []interface{})
}
```

The Dequeuer interface is implemented such that it'll maintain the FIFO, it'll read data from the store before the queue. These calls have the possibility of performing file i/o (e.g. if the store's memory has been depleted, but the there's still data on disk). Look at the durostore [documentation](http://github.com/antonio-alexander/go-durostore) for more information (this would use the Reader interface). These functions are DESTRUCTIVE.

```go
type Dequeuer interface {
    Dequeue() (item interface{}, underflow bool)
    DequeueMultiple(n int) (items []interface{})
    Flush() (items []interface{})
}
```

The Peeker interface implements the Dequerer interface, but in a NON-DESTRUCTIVE way. Use this in workflows where you need to know what the data is and it's order, but don't want to remove it from the store/queue.

```go
type Peeker interface {
    Peek() (items []interface{})
    PeekHead() (item interface{}, underflow bool)
    PeekFromHead(n int) (items []interface{})
}
```

The Enqueuer interface can be used to put data into the queue/store. In the event the queue overflows, the queue will be flushed and those items will be placed into the store (on disk) and any new data will be placed into the queue (maintaining order).

```go
type Enqueuer interface {
    Enqueue(item interface{}) (overflow bool)
    EnqueueMultiple(items []interface{}) (itemsRemaining []interface{}, overflow bool)
}
```

The Info interface can be used to determine how many items are in the queue+store as well as the capacity of the queue (capacity for the store is based on the size on disk rather than the number of items themselves).

```go
type Info interface {
    Length() (size int)
    Capacity() (capacity int)
}
```

The Event interface can be used to determine when data is put in or taken out of the queue, it mirrors the queue interfaces but is also aware of the store (so you get a signal whether the data comes from the store or the queue).

```go
type Event interface {
    GetSignalIn() (signal <-chan struct{})
    GetSignalOut() (signal <-chan struct{})
}
```

## Dequeue wrappers

Similar to the Read wrappers discussed in the go-durostore [documentation](http://github.com/antonio-alexander/go-durostore), you can wrap the Dequeue function to properly convert the data output from the queue. This is made MORE complex since the same data could be stored as a pointer, or as a slice of bytes depending on the state of the queue/store.

```go
package example

import (
    internal_example "github.com/antonio-alexander/go-durostore/internal/example"
    goqueue "github.com/antonio-alexander/go-queue"

    "github.com/pkg/errors"
)

func Dequeue(dequerer goqueue.Dequeuer) (*internal_example.Data, error) {
    item, underflow := dequerer.Dequeue()
    if underflow {
        return nil, nil
    }
    switch i := item.(type) {
    default:
        return nil, errors.Errorf("unsupported type %T", i)
    case *internal_example.Data:
        return i, nil
    case []byte:
        data := &internal_example.Data{}
        err := data.UnmarshalBinary(i)
        if err != nil {
            return nil, err
        }
        return data, nil
    }
}
```

## Producer/consumer design patterns

The producer/consumer design pattern can be applied identical to how it'd be applied with the queue interfaces. Duroqueue has the added ability to have a producer/consumer pattern across memory spaces (i.e. separate applications) as long as they have access to the same file i/o.

Things to keep in mind when trying to use duroqueue for producer/consume across memory spaces:

- The consumer cannot write data (it's generally unaware of the write index)
- The consumer will have to periodically read the indexes (since it's not doing the writing) using the UpdateIndexes() function since it can't take advantage of the the updated indexes when data is written
- The producer can't delete data since it doesn't know when the consumer has read all data, very important to ensure that the producer only interacts with the data non-destructively (e.g. don't use delete)
- Keep in mind that the consumer as-coded will automatically clean-up any files once all the data has been read, on the outside looking in, this can be confusing, but in general, it just means that when the producer attempts to write, if the directory is empty, it'll start from scratch

The UpdateIndexes() function is coded such that you can provide the size of the index as an input to avoid unnecessary file i/o (if no new indexes have been added by the producer). You'll need to create a go routine to periodically call that function and store the last size of those indexes:

```go
package example

import (
    "fmt"
    "sync"
    "time"

    durostore "github.com/antonio-alexander/go-durostore"
    duroqueue "github.com/antonio-alexander/go-durostore/queue"
)

func errorHandler(err error) {
    if err != nil {
        fmt.Println(err)
    }
}

func Main() {
    var wg sync.WaitGroup
    var err error

    //this configuration creates a queue size of one and ensures that
    // the producer/consumer queues/stores have the same configuration
    // and enabled file lockings
    queueSize := 1
    config := durostore.Configuration{
        Directory:   "./",
        MaxFiles:    10,
        MaxFileSize: durostore.MegaByteToByte(10),
        MaxChunk:    durostore.MegaByteToByte(1),
        FilePrefix:  "test",
        FileLocking: true,
    }
    //create the producer/consumer stores/queues with the given configuration
    // prune the directory prior to and set the error handler
    storeProducer := durostore.New(config)
    queueProducer := duroqueue.New(storeProducer, queueSize)
    queueProducer.SetErrorHandler(errorHandler)
    err = storeProducer.PruneDirectory()
    errorHandler(err)
    storeConsumer := durostore.New(config)
    queueConsumer := duroqueue.New(storeConsumer, queueSize)
    queueConsumer.SetErrorHandler(errorHandler)
    //create a go routine that will periodically update the indexes,
    // avoiding file i/o if the size of the indexes havn't been updated
    // It does this at 1Hz
    stopper := make(chan struct{})
    started := make(chan struct{})
    wg.Add(1)
    go func() {
        defer wg.Done()

        size, err := storeConsumer.UpdateIndexes()
        errorHandler(err)
        tCheck := time.NewTicker(time.Second)
        defer tCheck.Stop()
        close(started)
        for {
            select {
            case <-tCheck.C:
                size, err = storeConsumer.UpdateIndexes(size)
                errorHandler(err)
            case <-stopper:
                return
            }
        }
    }()
    <-started
    //TODO: implement your production/consumption business logic here
    //when you are done, close the stopper to stop the go routine and
    // wait until it returns, then clean-up and perform error handling
    close(stopper)
    wg.Wait()
    queueProducer.Close()
    queueConsumer.Close()
    err = storeProducer.Close()
    errorHandler(err)
    err = storeConsumer.Close()
    errorHandler(err)
}
```
