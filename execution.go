package durostore

import (
	internal "github.com/antonio-alexander/go-durostore/internal"
)

func findReadIndex(indexes map[uint64]*internal.Index) (readIndex uint64) {
	if len(indexes) > 0 {
		//KIM: if we don't have a valid readIndex it'll always return 0
		for i := range indexes {
			readIndex = i
			break
		}
		for i := range indexes {
			if i < readIndex {
				readIndex = i
			}
		}
	}

	return
}

func MegaByteToByte(n int64) int64 {
	return internal.MegaByteToByte(n)
}
