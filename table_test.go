package mysql2nsq

import (
	"testing"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/stretchr/testify/assert"
)

func TestNewTableMetaManager(t *testing.T) {
	t.Skip("依赖mysql特定表information_schema")
	db, err := gorm.Open("mysql", "root:@/information_schema?charset=utf8&parseTime=True&loc=Local")
	assert.Nil(t, err)
	defer db.Close()

	sc1 := SchemaConfig{
		Name:   "central-kitchen",
		Tables: []string{"picking_batch", "picking_batch_item"},
	}

	schema := Schema{
		Name: "central-kitchen",
		Tables: []Table{
			Table{
				Name: "picking_batch",
				Columns: []Column{
					Column{ColumnName: "id", OrdinalPosition: 1, IsNullable: "NO", DataType: "int"},
					Column{ColumnName: "batch_no", OrdinalPosition: 2, IsNullable: "NO", DataType: "varchar"},
					Column{ColumnName: "shop_id", OrdinalPosition: 3, IsNullable: "NO", DataType: "int"},
					Column{ColumnName: "operator_name", OrdinalPosition: 4, IsNullable: "NO", DataType: "varchar"},
					Column{ColumnName: "operator_id", OrdinalPosition: 5, IsNullable: "YES", DataType: "int"},
					Column{ColumnName: "created_at", OrdinalPosition: 6, IsNullable: "YES", DataType: "datetime"},
					Column{ColumnName: "updated_at", OrdinalPosition: 7, IsNullable: "YES", DataType: "datetime"},
					Column{ColumnName: "deleted_at", OrdinalPosition: 8, IsNullable: "YES", DataType: "datetime"},
				},
			},
			Table{
				Name: "picking_batch_item",
				Columns: []Column{
					Column{ColumnName: "id", OrdinalPosition: 1, IsNullable: "NO", DataType: "int"},
					Column{ColumnName: "batch_no", OrdinalPosition: 2, IsNullable: "NO", DataType: "varchar"},
					Column{ColumnName: "shop_id", OrdinalPosition: 3, IsNullable: "NO", DataType: "int"},
					Column{ColumnName: "code", OrdinalPosition: 4, IsNullable: "NO", DataType: "varchar"},
					Column{ColumnName: "number", OrdinalPosition: 5, IsNullable: "NO", DataType: "int"},
					Column{ColumnName: "operator_name", OrdinalPosition: 6, IsNullable: "NO", DataType: "varchar"},
					Column{ColumnName: "operator_id", OrdinalPosition: 7, IsNullable: "YES", DataType: "int"},
					Column{ColumnName: "created_at", OrdinalPosition: 8, IsNullable: "YES", DataType: "datetime"},
					Column{ColumnName: "updated_at", OrdinalPosition: 9, IsNullable: "YES", DataType: "datetime"},
					Column{ColumnName: "deleted_at", OrdinalPosition: 10, IsNullable: "YES", DataType: "datetime"},
				},
			},
		},
	}

	mng, err := NewTableMetaManager(db, []SchemaConfig{sc1})
	assert.Nil(t, err)
	assert.NotNil(t, mng)
	assert.Equal(t, []Schema{schema}, mng.schemas)
}

func TestReadAllTableNamesInSchema(t *testing.T) {
	t.Skip("依赖mysql特定表information_schema")
	db, err := gorm.Open("mysql", "mysql2nsq:mysql2nsq@(127.0.0.1:3309)/information_schema?charset=utf8&parseTime=True&loc=Local")
	assert.Nil(t, err)
	defer db.Close()

	sc := SchemaConfig{
		Name:   "ck",
		Tables: []string{"picking_batch", "picking_batch_item"},
	}
	tmm, err := NewTableMetaManager(db, []SchemaConfig{sc})
	assert.Nil(t, err)

	tableNames, err := tmm.readAllTableNamesInSchema("ck")
	assert.Nil(t, err)
	assert.Equal(t, []string{"operator", "order", "order_item", "picking_batch", "picking_batch_item", "product", "shop", "shop_operator", "sms_queue", "sms_scene", "user"}, tableNames)
}
