package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/gofrs/uuid"
	"github.com/hiwjd/mysql2nsq"
	"github.com/nsqio/go-nsq"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/replication"
)

var (
	logPath         string
	host            string
	port            uint
	user            string
	password        string
	GTIDSetFilePath string
	initGTIDSetStr  string
	tableFilePath   string
	nsqdAddr        string
)

func init() {
	flag.StringVar(&logPath, "lp", "./logs/mysql2nsq.log", "log path")
	flag.StringVar(&host, "host", "127.0.0.1", "mysql master host")
	flag.UintVar(&port, "port", 3306, "mysql master port")
	flag.StringVar(&user, "user", "root", "mysql master user")
	flag.StringVar(&password, "password", "", "mysql master password")
	flag.StringVar(&GTIDSetFilePath, "fp", "current_gtidset.db", "file to storage GTIDSet")
	flag.StringVar(&initGTIDSetStr, "i", "", "init GTIDSet")
	flag.StringVar(&tableFilePath, "tfp", "./table.json", "a json file that storage table column info")
	flag.StringVar(&nsqdAddr, "nsqdAddr", "127.0.0.1:4150", "nsqd address")

	fh, err := log.NewFileHandler(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND)
	if err != nil {
		panic("open log file failed.")
	}
	log.SetDefaultLogger(log.NewDefault(fh))
	log.SetLevel(log.LevelTrace)
}

func main() {
	flag.Parse()

	// 表字段定义
	tableMap, err := mysql2nsq.ReadFromFileToTable(tableFilePath)
	if err != nil {
		log.Errorf("Read table map failed: %s\n", err)
		os.Exit(1)
	}

	fmt.Println(tableMap)

	// GTIDSet存储器
	storage, err := mysql2nsq.NewGTIDSetStorage(GTIDSetFilePath, initGTIDSetStr)
	if err != nil {
		log.Errorf("Create GTIDSetStorage failed: %s\n", err)
		os.Exit(1)
	}

	// 读取已经同步过的binlog GTIDSet
	GTIDSet, err := storage.Read()
	if err != nil {
		log.Errorf("Read init GTIDSet failed: %s\n", err)
		os.Exit(1)
	}

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddr, nsqConfig)
	if err != nil {
		log.Errorf("New nsq producer failed: %s\n", err)
		os.Exit(1)
	}

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: 102,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
	}
	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(GTIDSet)
	if err != nil {
		log.Errorf("Start sync failed: %s\n", err)
		os.Exit(1)
	}

	log.Infof("Start syncing from GTIDSet: %s\n", GTIDSet)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

loop:
	for {
		select {
		case <-c:
			log.Infof("Receive interrupt signal, prepare to exit\n")
			// if ok, fstore := store.(*mysql2nsq.fileStorage)
			syncer.Close()
			break loop
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ev, err := streamer.GetEvent(ctx)
			cancel()

			if err == nil {
				// Dump event
				// ev.Dump(os.Stdout)

				switch ev.Header.EventType {
				case replication.GTID_EVENT:
					// 更新GTIDSet
					if evt, ok := ev.Event.(*replication.GTIDEvent); ok {
						u, _ := uuid.FromBytes(evt.SID)
						GTID := fmt.Sprintf("%s:%d", u.String(), evt.GNO)
						if err := storage.Update(GTID); err != nil {
							log.Errorf("Update GTID failed %s: %s\n", GTID, err)
						}
					} else {
						log.Errorf("Convert event to replication.GTIDEvent failed, event: \n")
					}
					break
				case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
					// 发送新增、删除、修改数据到nsq
					ev.Dump(os.Stdout)
					dc, err := mysql2nsq.NewDataChangedFromBinlogEvent(ev, tableMap)
					if err != nil {
						log.Errorf("转换成DataChanged出错了：%s\n", err)
					} else {
						if err = producer.Publish(dc.Schema, dc.Encode()); err != nil {
							log.Errorf("发布至nsq失败：%s\n", err)
						}
					}
					break
				}
			} else if err == replication.ErrNeedSyncAgain {
				log.Errorln("GetEvent with err: replication.ErrNeedSyncAgain")
				continue
			} else if err == context.DeadlineExceeded {
				// log.Errorln("GetEvent with err: context.DeadlineExceeded")
				continue
			} else {
				log.Errorf("GetEvent with err: %s, exit.\n", err)
			}
		}
	}
}
