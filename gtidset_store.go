package mysql2nsq

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
)

var (
	// ErrSyncToFile 表示更新GTIDSet时，由于写入到文件中的长度不匹配失败
	ErrSyncToFile = errors.New("Update failed due to mismatch length written in file sync")
)

// GTIDSetStorage 是维护最新的GTIDSet
type GTIDSetStorage interface {

	// 当收到`GTIDEvent`事件时，更新GTIDSet
	Update(GTIDStr string) error

	// 读取最新的GTIDSet
	Read() (mysql.GTIDSet, error)
}

// NewGTIDSetStorage 构造一个GTIDSetStorage
// filePath 是存储GTIDSet的文件路径
// initGTIDSetStr 是初始GTIDSet字符串，只在filePath指定的文件中没有读到GTIDSet时使用
func NewGTIDSetStorage(filePath string, initGTIDSetStr string) (GTIDSetStorage, error) {
	return newFileStorage(filePath, initGTIDSetStr)
}

func newFileStorage(filePath string, initGTIDSetStr string) (*fileStorage, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1024)
	var GTIDSet mysql.GTIDSet

	// 从文件中读取GTIDSet
	n, err := file.Read(buf[0:])
	if err == nil {
		// 使用文件值
		GTIDSet, err = mysql.ParseMysqlGTIDSet(string(buf[0:n]))
	} else if err == io.EOF {
		// 使用初始值
		GTIDSet, err = mysql.ParseMysqlGTIDSet(initGTIDSetStr)
	}

	if err != nil || GTIDSet == nil {
		file.Close()
		return nil, err
	}

	return &fileStorage{
		file:          file,
		lock:          &sync.Mutex{},
		buf:           buf,
		latestGTIDSet: GTIDSet,
	}, nil
}

type fileStorage struct {
	file          *os.File
	lock          sync.Locker
	buf           []byte
	latestGTIDSet mysql.GTIDSet
}

// Update implement GTIDSetStorage
func (s *fileStorage) Update(GTIDStr string) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err = s.latestGTIDSet.Update(GTIDStr); err != nil {
		return
	}

	// todo 期望写入到文件的操作可以在另个线程执行（或其他方式）
	err = s.writeToFile(s.latestGTIDSet)
	return
}

// Read implement GTIDSetStorage
func (s *fileStorage) Read() (mysql.GTIDSet, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 重置偏移量
	if _, err := s.file.Seek(0, 0); err != nil {
		return nil, err
	}

	n, err := s.file.Read(s.buf[0:])
	if err != nil && err == io.EOF {
		return mysql.ParseMysqlGTIDSet("")
	}

	return mysql.ParseMysqlGTIDSet(string(s.buf[0:n]))
}

func (s *fileStorage) Close() error {
	return s.file.Close()
}

func (s *fileStorage) writeToFile(GTIDSet mysql.GTIDSet) (err error) {
	b := []byte(GTIDSet.String())

	var n int
	if n, err = s.file.WriteAt(b, 0); err != nil {
		return
	}

	if n != len(b) {
		err = ErrSyncToFile
		return
	}

	return
}
