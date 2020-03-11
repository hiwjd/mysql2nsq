package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gofrs/uuid"
	"github.com/hiwjd/mysql2nsq"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/nsqio/go-nsq"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/replication"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "c", "./config.toml", "配置文件路径")
}

func main() {
	flag.Parse()

	var config mysql2nsq.Config
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		panic(err.Error())
	}

	var w log.Handler
	switch config.Log.Output {
	case "stdout":
		w = os.Stdout
		break
	default:
		w = &lumberjack.Logger{
			Filename:   config.Log.Output,
			MaxSize:    config.Log.MaxSize, // megabytes
			MaxBackups: config.Log.MaxBackups,
			MaxAge:     config.Log.MaxAge,   // days
			Compress:   config.Log.Compress, // disabled by default
		}
	}

	logger := log.NewDefault(w)
	log.SetDefaultLogger(logger)
	log.SetLevel(log.LevelTrace)

	// 数据库
	mysqlDSN := fmt.Sprintf(
		"%s:%s@(%s:%d)/information_schema?charset=utf8&parseTime=True&loc=Local",
		config.Mysql.User,
		config.Mysql.Password,
		config.Mysql.Host,
		config.Mysql.Port,
	)
	db, err := gorm.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("打开数据库失败: %s\n", err.Error())
	}
	db.SetLogger(logger)
	db.LogMode(false)
	defer db.Close()

	// 表字段定义
	tmm, err := mysql2nsq.NewTableMetaManager(db, config.Schemas)
	if err != nil {
		log.Fatalf("表结构获取失败: %s\n", err.Error())
	}

	// GTIDSet存储器
	storage, err := mysql2nsq.NewGTIDSetStorage(config.Storage.FilePath, config.Storage.InitGTIDSet)
	if err != nil {
		log.Fatalf("Create GTIDSetStorage failed: %s\n", err)
	}

	// 读取已经同步过的binlog GTIDSet
	GTIDSet, err := storage.Read()
	if err != nil {
		log.Fatalf("Read init GTIDSet failed: %s\n", err)
	}

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(config.NsqdAddr, nsqConfig)
	if err != nil {
		log.Fatalf("New nsq producer failed: %s\n", err)
	}

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: config.Mysql.ServerID,
		Flavor:   "mysql",
		Host:     config.Mysql.Host,
		Port:     config.Mysql.Port,
		User:     config.Mysql.User,
		Password: config.Mysql.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(GTIDSet)
	if err != nil {
		log.Fatalf("Start sync failed: %s\n", err)
	}

	log.Infof("Start syncing from GTIDSet: %s\n", GTIDSet)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

loop:
	for {
		select {
		case <-c:
			log.Infof("Receive interrupt signal, prepare to exit\n")
			syncer.Close()
			break loop
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ev, err := streamer.GetEvent(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// 超时了，继续等待
					continue
				}
				log.Errorf("等待binlog时触发错误: %s\n", err.Error())
				break loop
			}

			switch e := ev.Event.(type) {
			case *replication.GTIDEvent:
				// 更新GTIDSet
				u, _ := uuid.FromBytes(e.SID)
				GTID := fmt.Sprintf("%s:%d", u.String(), e.GNO)
				if err := storage.Update(GTID); err != nil {
					log.Errorf("更新GTID失败 %s: %s\n", GTID, err.Error())
				}
				break
			case *replication.RowsEvent:
				// 发送新增、删除、修改数据到nsq
				dc, err := mysql2nsq.NewDataChangedFromBinlogEvent(ev, tmm)
				if err != nil {
					log.Errorf("转换成DataChanged出错了：%s\n", err.Error())
				} else {
					log.Debugf("准备发送数据: %+v\n", dc)
					if err = producer.Publish(dc.Schema, dc.Encode()); err != nil {
						log.Errorf("发布至nsq失败：%s\n", err)
					}
				}
				break
			}
		}
	}
}
