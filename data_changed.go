package mysql2nsq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/siddontang/go-mysql/replication"
)

var (
	ErrInvalidEventType   = errors.New("invalid event type")
	ErrConvertToRowsEvent = errors.New("Convert event to replication.RowsEvent failed")
	ErrTableNotFound      = errors.New("table not found")
)

// Action represents insert,update,delete
type Action string

var (
	// INSERT insert
	INSERT Action = "INSERT"
	// UPDATE update
	UPDATE Action = "UPDATE"
	// DELETE delete
	DELETE Action = "DELETE"
)

// DataChanged represents binlog RowEvent
type DataChanged struct {
	Schema string
	Table  string
	Action Action
	Rows   []map[string]interface{}
}

func (dc DataChanged) Encode() []byte {
	bs, err := json.Marshal(dc)
	if err != nil {

	}
	return bs
}

func (dc *DataChanged) Decode(bs []byte) error {
	return json.Unmarshal(bs, dc)
}

// NewDataChangedFromBinlogEvent construct DataChanged from BinlogEvent
func NewDataChangedFromBinlogEvent(ev *replication.BinlogEvent, tableMap map[string]*Table) (*DataChanged, error) {
	dc := &DataChanged{}

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv2:
		dc.Action = INSERT
		break
	case replication.UPDATE_ROWS_EVENTv2:
		dc.Action = UPDATE
		break
	case replication.DELETE_ROWS_EVENTv2:
		dc.Action = DELETE
		break
	default:
		return nil, ErrInvalidEventType
	}

	var ok bool

	var evt *replication.RowsEvent
	if evt, ok = ev.Event.(*replication.RowsEvent); !ok {
		return nil, ErrConvertToRowsEvent
	}

	dc.Schema = string(evt.Table.Schema)
	dc.Table = string(evt.Table.Table)
	tableMapID := fmt.Sprintf("%s-%s", dc.Schema, dc.Table)

	var tbl *Table
	if tbl, ok = tableMap[tableMapID]; !ok {
		return nil, ErrTableNotFound
	}

	rows := make([]map[string]interface{}, len(evt.Rows))

	for i, row := range evt.Rows {
		r := make(map[string]interface{})

		for j, d := range row {
			col, _ := tbl.GetColumnByIndex(j)
			r[col.Name] = d
		}

		rows[i] = r
	}

	dc.Rows = rows

	return dc, nil
}
