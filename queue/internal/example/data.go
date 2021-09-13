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
