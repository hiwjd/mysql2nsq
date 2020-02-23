package mysql2nsq

import (
	"io/ioutil"
	"testing"
)

func TestReadFromFileToTable(t *testing.T) {
	fn := "./test-table.json"

	// 准备数据
	if err := ioutil.WriteFile(fn, []byte(`{"db1-user":{"Cols":[{"Name":"id"},{"Name":"name"}]}}`), 0644); err != nil {
		t.Fatal("prepare data failed.")
	}

	data, err := ReadFromFileToTable(fn)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != 1 {
		t.Fatal("1")
	}

	table := data["db1-user"]
	if table == nil {
		t.Fatal("2")
	}

	expectNames := []string{"id", "name"}
	for i := 0; i < 2; i++ {
		col, err := table.GetColumnByIndex(i)
		if err != nil {
			t.Fatal("3")
		}
		if col == nil || col.Name != expectNames[i] {
			t.Fatal("4")
		}
	}
}
