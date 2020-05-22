package mysql2nsq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	dc := &DataChanged{}
	dc.Action = INSERT
	dc.Schema = "db1"
	dc.Table = "user"
	dc.Rows = []map[string]interface{}{{"id": 1, "name": "hiwjd"}}

	bs, err := dc.Encode()
	assert.Nil(t, err)

	dc2 := &DataChanged{}
	err = dc2.Decode(bs)
	if err != nil {
		t.Fatal(err)
	}

	if dc.Action != dc2.Action || dc.Schema != dc2.Schema || dc.Table != dc2.Table {
		t.Fatal("not match")
	}

	if len(dc.Rows) != len(dc2.Rows) {
		t.Fatal("not match")
	}

	n := len(dc.Rows)

	for i := 0; i < n; i++ {
		r1 := dc.Rows[i]
		r2 := dc2.Rows[i]

		if len(r1) != len(r2) {
			t.Fatal("not match")
		}

		// if r1["id"].(int) != r2["id"].(int) || r1["name"].(string) != r2["name"].(string) {
		// 	t.Fatal("not match")
		// }
	}
}
