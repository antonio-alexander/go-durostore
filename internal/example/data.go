package example

import (
	"encoding/json"

	"github.com/antonio-alexander/go-durostore"
)

type Data struct {
	Int    int    `json:"Int"`
	String string `json:"String"`
}

func (d *Data) MarshalBinary() ([]byte, error) {
	return json.MarshalIndent(d, "", " ")
}

func (d *Data) UnmarshalBinary(bytes []byte) error {
	return json.Unmarshal(bytes, d)
}

func Read(reader durostore.Reader, index ...uint64) *Data {
	data := &Data{}
	bytes, err := reader.Read(index...)
	if err != nil {
		return nil
	}
	err = data.UnmarshalBinary(bytes)
	if err != nil {
		return nil
	}
	return data
}
