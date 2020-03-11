package mysql2nsq

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	data := `
nsqd_addr = "127.0.0.1:4150"

[log]
  output = "stdout"
  max_size = 100
  max_backups = 7
  compress = true

[mysql]
  host = "127.0.0.1"
  port = 3306
  user = "root"
  password = ""

[[schema]]
  name = "schema1"
  tables = ["table1"]

[[schema]]
  name = "schema2"
  tables = ["table1", "table3"]

[storage]
  file_path = "./gtidset.db"
  init_gtidset = "36c0fcec-5447-11ea-8dc1-0242ac110002:1-7713"
	  `

	var config Config
	_, err := toml.Decode(data, &config)
	assert.Nil(t, err)
	assert.Equal(t, "stdout", config.Log.Output)
	assert.Equal(t, 100, config.Log.MaxSize)
	assert.Equal(t, 7, config.Log.MaxBackups)
	assert.Equal(t, true, config.Log.Compress)

	assert.Equal(t, "127.0.0.1", config.Mysql.Host)
	assert.Equal(t, uint16(3306), config.Mysql.Port)
	assert.Equal(t, "root", config.Mysql.User)
	assert.Equal(t, "", config.Mysql.Password)

	assert.Equal(t, "127.0.0.1:4150", config.NsqdAddr)

	assert.Equal(t, 2, len(config.Schemas))

	assert.Equal(t, "schema1", config.Schemas[0].Name)
	assert.Equal(t, 1, len(config.Schemas[0].Tables))
	assert.Equal(t, "table1", config.Schemas[0].Tables[0])

	assert.Equal(t, "schema2", config.Schemas[1].Name)
	assert.Equal(t, 2, len(config.Schemas[1].Tables))
	assert.Equal(t, "table1", config.Schemas[1].Tables[0])
	assert.Equal(t, "table3", config.Schemas[1].Tables[1])

	assert.Equal(t, "./gtidset.db", config.Storage.FilePath)
	assert.Equal(t, "36c0fcec-5447-11ea-8dc1-0242ac110002:1-7713", config.Storage.InitGTIDSet)
}
