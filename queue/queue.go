package queue

import (
	"sync"

	durostore "github.com/antonio-alexander/go-durostore"
	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"
)

type duroqueue struct {
	sync.RWMutex
	storeManager interface {
		durostore.Owner
	}
	store interface {
		durostore.Reader
		durostore.Writer
		durostore.Info
	}
	queue interface {
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Event
		goqueue.Peeker
		goqueue.Info
		goqueue.Owner
	}
	chData     chan struct{}
	errHandler ErrorHandler
}

//New can be used to create an instance of duroqueue, if durostore.Configuration
// is provided, it'll create an new instance of durostore, if the store is provided
// it'll otherwise be used
func New(parameters ...interface{}) interface {
	goqueue.Enqueuer
	goqueue.Dequeuer
	goqueue.Peeker
	goqueue.Event
	goqueue.Info
	goqueue.Owner
	Duroqueue
} {

	d := &duroqueue{
		chData: make(chan struct{}),
	}
	for _, p := range parameters {
		switch v := p.(type) {
		case int:
			d.queue = finite.New(v)
		case durostore.Configuration:
			s := durostore.New(v)
			d.store, d.storeManager = s, s
		case interface {
			durostore.Reader
			durostore.Writer
			durostore.Info
		}:
			d.store = v
		}
	}
	if d.store == nil {
		panic("the store is nil, provide configuration or durostore")
	}
	if d.queue == nil {
		d.queue = finite.New(0)
	}
	return d
}

func (d *duroqueue) errorHandler(err error) {
	if d.errHandler != nil {
		d.errHandler(err)
	}
}

func (d *duroqueue) sendSignalOut() (sent bool) {
	//send empty (an empty struct variable) in a non-blocking manner
	// since the channel is un-buffered, we don't want it to block
	// until someone reads it
	select {
	case d.chData <- struct{}{}:
		sent = true
	default:
	}

	return
}

func (d *duroqueue) peekHead() (item interface{}, underflow bool) {
	var err error

	//this is non-destructive, and will pull the oldest value
	// from the durostore/queue as expected, because we can't
	// "communicate" errors, except by channel, so we set
	// underflow to true if there's a problem with the store
	defer func() {
		if err != nil {
			d.errorHandler(err)
			underflow = true
		}
	}()
	if d.store.Length() > 0 {
		if item, err = d.store.Read(); err != nil {
			underflow = true
		}
		return
	}
	return d.queue.PeekHead()
}

func (d *duroqueue) dequeue() (item interface{}, underflow bool) {
	var err error

	//we want to prioritize offloaded data by reading data from the
	// minimum index, we know there's data IF the index map isn't
	// empty, if this is the case, we read the minimum index
	// and "destroy" the data on disk, then incremment, there's
	// an opportunity to read the next chunk of data (if any)
	// in the event the map is empty after the deletion
	defer func() {
		if err != nil {
			d.errorHandler(err)
			underflow = true
		}
	}()
	if d.store.Length() > 0 {
		if item, err = d.store.Read(); err != nil {
			return
		}
		if item != nil {
			if err = d.store.Delete(); err != nil {
				return
			}
			d.sendSignalOut()

			return
		}
	}
	if item, underflow = d.queue.Dequeue(); !underflow {
		d.sendSignalOut()
	}

	return
}

func (d *duroqueue) Close() (remainingElements []interface{}) {
	d.Lock()
	defer d.Unlock()

	if remainingElements := d.queue.Close(); len(remainingElements) > 0 {
		if _, err := d.store.Write(remainingElements...); err != nil {
			d.errorHandler(err)
		}
	}
	select {
	default:
		close(d.chData)
	case <-d.chData:
	}
	if d.storeManager != nil {
		if err := d.storeManager.Close(); err != nil {
			d.errorHandler(err)
		}
	}
	d.errHandler = nil

	return
}

func (d *duroqueue) SetErrorHandler(errHandler ErrorHandler) {
	d.Lock()
	defer d.Unlock()

	d.errHandler = errHandler
}

func (d *duroqueue) Enqueue(item interface{}) (overflow bool) {
	d.Lock()
	defer d.Unlock()

	//attempt to enqueue, if overflow, flush the queue
	// write the flushed data, and then attempt to enqueue
	// again
	if overflow = d.queue.Enqueue(item); overflow {
		items := d.queue.Flush()
		if _, err := d.store.Write(items); err != nil {
			d.errorHandler(err)
			overflow = true

			return
		}
		overflow = d.queue.Enqueue(item)
	}

	return
}

//KIM: when this is called, it should only return overflow if there was an error and remaining
// item should always be empty
func (d *duroqueue) EnqueueMultiple(items []interface{}) (remainingElements []interface{}, overflow bool) {
	d.Lock()
	defer d.Unlock()

	//attempt to enqueue multiple items, if unable to enqueue all,
	// flush the queue, write those items, and attempt to enqueue again
	// until failure or success
	for remainingElements, overflow = d.queue.EnqueueMultiple(items); len(remainingElements) > 0; {
		items := d.queue.Flush()
		if _, err := d.store.Write(items); err != nil {
			d.errorHandler(err)
			overflow = true

			return
		}
	}
	remainingElements = nil

	return
}

func (d *duroqueue) Peek() (items []interface{}) {
	d.Lock()
	defer d.Unlock()
	item, _ := d.peekHead()
	return []interface{}{item}
}

func (d *duroqueue) PeekHead() (item interface{}, underflow bool) {
	d.Lock()
	defer d.Unlock()
	return d.peekHead()
}

func (d *duroqueue) PeekFromHead(n int) (items []interface{}) {
	d.Lock()
	defer d.Unlock()
	item, _ := d.peekHead()
	return []interface{}{item}
}

//KIM: dequeue is DESTRUCTIVE for durostore, once de-queued, it's no
// longer stored on disk, use the Peek() functions instead if you
// want to perform a non-destructive read
func (d *duroqueue) Dequeue() (item interface{}, underflow bool) {
	d.Lock()
	defer d.Unlock()
	return d.dequeue()
}

func (d *duroqueue) DequeueMultiple(n int) (items []interface{}) {
	d.Lock()
	defer d.Unlock()

	//read as many items as n OR until an underflow is experienced
	for i := 0; i < n; i++ {
		var item interface{}
		var underflow bool

		if item, underflow = d.dequeue(); underflow {
			return
		}
		items = append(items, item)
	}

	return
}

//KIM: flush is intended to empty the queue, but because all of
// the available data may not be loaded in memory, this will
// only destructively read everything that's in-memory, in practice
// if the goal is to read all of the items on disk with the flush
// command, you'll need to call until the number of items returned
// is zero OR ensure that configuration loads ALL data on disk into
// memory rather than a chunk
func (d *duroqueue) Flush() (items []interface{}) {
	d.Lock()
	defer d.Unlock()

	//attempt to read all of the items in the data map if
	// there are items, DO NOT attempt to flush qIn and
	// append to the end since there may still be data
	// on disk that needs to be read in
	for underflow := false; underflow; {
		var item interface{}

		if item, underflow = d.dequeue(); underflow {
			break
		}
		items = append(items, item)
	}
	items = append(items, d.queue.Flush()...)

	return
}

func (d *duroqueue) GetSignalIn() (signal <-chan struct{}) {
	d.RLock()
	defer d.RUnlock()
	return d.queue.GetSignalIn()
}

func (d *duroqueue) GetSignalOut() (signal <-chan struct{}) {
	d.RLock()
	defer d.RUnlock()
	return d.chData
}

func (d *duroqueue) Length() (size int) {
	d.RLock()
	defer d.RUnlock()
	return d.store.Length() + d.queue.Length()
}

func (d *duroqueue) Capacity() int {
	d.RLock()
	defer d.RUnlock()
	return d.queue.Capacity()
}
