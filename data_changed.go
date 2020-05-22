package mysql2nsq

import (
	"encoding/json"
	"errors"

	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/replication"
)

var (
	ErrInvalidEventType   = errors.New("invalid event type")
	ErrConvertToRowsEvent = errors.New("Convert event to replication.RowsEvent failed")
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

func (dc DataChanged) Encode() ([]byte, error) {
	return json.Marshal(dc)
}

func (dc *DataChanged) Decode(bs []byte) error {
	return json.Unmarshal(bs, dc)
}

// NewDataChangedFromBinlogEvent construct DataChanged from BinlogEvent
func NewDataChangedFromBinlogEvent(ev *replication.BinlogEvent, tmm *TableMetaManager) (*DataChanged, error) {
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

	tbl, err := tmm.Query(dc.Schema, dc.Table)
	if err != nil {
		log.Debugf("获取表定义失败:%s - %s", dc.Schema, dc.Table)
		return nil, err
	}

	rows := make([]map[string]interface{}, len(evt.Rows))

	for i, row := range evt.Rows {
		r := make(map[string]interface{})

		for j, v := range row {
			col, err := tbl.Query(j)
			if err != nil {
				return nil, err
			}

			r[col.ColumnName] = col.Format(v)
		}

		rows[i] = r
	}

	dc.Rows = rows

	return dc, nil
}
