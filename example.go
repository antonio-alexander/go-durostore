package durostore

import (
	"encoding/json"
)

type Example struct {
	Int    int     `json:"int,omitempty"`
	Float  float64 `json:"float,omitempty"`
	String string  `json:"string,omitempty"`
}

func (e *Example) MarshalBinary() ([]byte, error) {
	return json.MarshalIndent(e, "", " ")
}

func (e *Example) UnmarshalBinary(bytes []byte) error {
	return json.Unmarshal(bytes, e)
}

func ExampleRead(reader Reader, index ...uint64) *Example {
	example := &Example{}
	bytes, err := reader.Read(index...)
	if err != nil {
		return nil
	}
	if err := example.UnmarshalBinary(bytes); err != nil {
		return nil
	}
	return example
}
