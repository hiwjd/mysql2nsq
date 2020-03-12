package mysql2nsq

import (
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/siddontang/go-log/log"
)

var (
	// ErrNotFound 表示没有知道表数据
	ErrNotFound = errors.New("table not found")
)

// TableMetaManager 管理表结构
type TableMetaManager struct {
	db            *gorm.DB
	schemaConfigs []SchemaConfig
	schemas       []Schema
}

// NewTableMetaManager 返回TableMetaManager实例
func NewTableMetaManager(db *gorm.DB, schemaConfigs []SchemaConfig) (*TableMetaManager, error) {
	tmm := &TableMetaManager{db: db, schemaConfigs: schemaConfigs}

	var err error
	if tmm.schemas, err = tmm.buildSchemas(); err != nil {
		return nil, err
	}

	return tmm, nil
}

// Query 根据库名和表名查找表数据
func (tmm TableMetaManager) Query(schemaName, tableName string) (*Table, error) {
	for _, sc := range tmm.schemas {
		if sc.Name == schemaName {
			for _, tbl := range sc.Tables {
				if tbl.Name == tableName {
					return &tbl, nil
				}
			}
			break
		}
	}

	return nil, ErrNotFound
}

func (tmm *TableMetaManager) buildSchemas() ([]Schema, error) {
	var schemas []Schema
	for _, schema := range tmm.schemaConfigs {
		if len(schema.Tables) == 0 {
			// 查询该库所有表
			tbls, err := tmm.readAllTableNamesInSchema(schema.Name)
			if err != nil {
				return nil, err
			}
			schema.Tables = tbls
		}
		var tables []Table
		for _, tableName := range schema.Tables {
			var columns []Column
			if err := tmm.db.Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema.Name, tableName).Order("ORDINAL_POSITION ASC").Find(&columns).Error; err != nil {
				return nil, err
			}

			table := Table{}
			table.Columns = columns
			table.Name = tableName

			tables = append(tables, table)
		}

		sc := Schema{}
		sc.Name = schema.Name
		sc.Tables = tables
		schemas = append(schemas, sc)
	}

	return schemas, nil
}

func (tmm TableMetaManager) readAllTableNamesInSchema(schemaName string) ([]string, error) {
	var tableNames []string
	if err := tmm.db.Model(&tbl{}).Where("TABLE_SCHEMA=?", schemaName).Pluck("TABLE_NAME", &tableNames).Error; err != nil {
		return nil, err
	}
	return tableNames, nil
}

func (tmm TableMetaManager) Dump(w io.Writer) {
	if bs, err := json.Marshal(tmm.schemas); err != nil {
		w.Write([]byte("dump err: " + err.Error()))
	} else {
		w.Write(bs)
	}
}

var colValueFormat = map[string]func(interface{}) interface{}{
	"datetime": func(v interface{}) interface{} {
		if v == nil {
			return v
		}

		if s, ok := v.(string); ok {
			t, err := time.Parse("2006-01-02 15:04:05", s)
			if err != nil {
				log.Errorf("ERROR format column value of datetime type failed: %s\n", err.Error())
				return v
			}
			return t
		}

		return v
	},
}

// Column 表示mysql字段
type Column struct {
	ColumnName      string `gorm:"column:COLUMN_NAME"`
	OrdinalPosition int    `gorm:"column:ORDINAL_POSITION"`
	IsNullable      string `gorm:"column:IS_NULLABLE"`
	DataType        string `gorm:"column:DATA_TYPE"`
}

// Format 把字段值处理成合适的类型
//
// 一个场景：mysql中datetime类型字段，从binlog中获取得到的是"2006-01-02 15:04:05"格式的string
// json序列化后：{"date":"2020-03-10 15:04:05"}
// 如果定义time.Time类型去反序列化会因为格式和time.Time默认的格式不符导致失败
//   type Row struct {
//     Date time.Time `json:"date"`
//   }
//
// Format内把mysql类型是datetime的转成time.Time后返回来解决
//
// 添加更多的转换到`colValueFormat`
func (c Column) Format(v interface{}) interface{} {
	if format, ok := colValueFormat[c.DataType]; ok {
		return format(v)
	}
	return v
}

// TableName 定义表名
func (Column) TableName() string {
	return "COLUMNS"
}

// Table 表示表
type Table struct {
	Name    string
	Columns []Column
}

// Schema 表示库
type Schema struct {
	Name   string
	Tables []Table
}

// Query 根据下标获取Column
func (t Table) Query(index int) (*Column, error) {
	if index < 0 || index >= len(t.Columns) {
		return nil, ErrNotFound
	}

	return &t.Columns[index], nil
}

type tbl struct {
	Name string `gorm:"column:TABLE_NAME"`
}

func (tbl) TableName() string {
	return "TABLES"
}
