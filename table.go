package mysql2nsq

import (
	"encoding/json"
  "errors"
  "io/ioutil"
)

var (
  ErrColumnNotFound = errors.New("column not found")
)

// Column 定义字段
type Column struct {
  Name string
}

// Table 定义表字段
type Table struct {
  Cols []*Column
}

// GetColumnByIndex get column by index
func (t Table) GetColumnByIndex(i int) (*Column, error) {
  len := len(t.Cols)
  if i < 0 || i > len - 1 {
    return nil, ErrColumnNotFound
  }

  return t.Cols[i], nil
}

// ReadFromFileToTable 从文件中读取表字段定义
func ReadFromFileToTable(fn string) (map[string]*Table, error) {
  bs, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
  }

  // table := &Table{}
  var data map[string]*Table
  if err = json.Unmarshal(bs, &data); err != nil {
    return nil, err
  }

  return data, nil
}
