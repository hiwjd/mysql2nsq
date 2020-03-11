package mysql2nsq

// Config 是配置
type Config struct {
  Log LogConfig `toml:"log"`
  Mysql MysqlConfig `toml:"mysql"`
  NsqdAddr string `toml:"nsqd_addr"`
  Schemas []SchemaConfig `toml:"schema"`
  Storage GTIDSetStorageConfig `toml:"storage"`
}

// LogConfig 是日志配置
type LogConfig struct {
	Output     string `toml:"output"`      // 文件路径（例子：log/http.log）或者`stdout`
	MaxSize    int    `toml:"max_size"`    // 单个日志文件的大小上限，单位MB
	MaxBackups int    `toml:"max_backups"` // 最多保留几个日志文件
	Compress   bool   `toml:"compress"`    // 是否压缩
}

// MysqlConfig 是mysql配置
type MysqlConfig struct {
  Host string `toml:"host"`
  Port uint16 `toml:"port"`
  User string `toml:"user"`
  Password string `toml:"password"`
}

// SchemaConfig 是库配置
type SchemaConfig struct {
  Name string `toml:"name"`
  Tables []string `toml:"tables"`
}

// GTIDSetStorageConfig 是记录GTIDSet的Storage的配置
type GTIDSetStorageConfig struct {
  FilePath string `toml:"file_path"`
  InitGTIDSet string `toml:"init_gtidset"`
}
