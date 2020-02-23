package mysql2nsq

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkSave(b *testing.B) {
	storage, _ := newFileStorage("current_gtidset", "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		storage.Update("36c0fcec-5447-11ea-8dc1-0242ac110002:1-7294")
	}

	storage.Close()
}

func BenchmarkUpdateAndRead(b *testing.B) {
	storage, _ := newFileStorage("current_gtidset", "36c0fcec-5447-11ea-8dc1-0242ac110002:1-7294")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage.Update("36c0fcec-5447-11ea-8dc1-0242ac110002:1-7294")
		}
	})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := storage.Read()
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func TestUpdateAndRead(t *testing.T) {
	storage, _ := newFileStorage("current_gtidset", "")
	defer storage.Close()

	s0 := "36c0fcec-5447-11ea-8dc1-0242ac110002:1-7294"
	storage.Update(s0)

	s, err := storage.Read()
	if err != nil {
		t.Fatalf("err: %s\n", err)
	}

	if s.String() != s0 {
		t.Fatalf("read: %s not equal to %s\n", s, s0)
	}
}

func TestNewFileStorage(t *testing.T) {
	fn := "file-has-gtidset"
	GTIDSet0 := "36c0fcec-5447-11ea-8dc1-0242ac110002:1-7294"
	err := writeFile(fn, GTIDSet0)
	if err != nil {
		t.FailNow()
	}
	fmt.Println("=====xxxx")

	storage, _ := newFileStorage(fn, "")
	defer storage.Close()

	GTIDSet, _ := storage.Read()
	if GTIDSet.String() != GTIDSet0 {
		t.Error("")
	}
}

func writeFile(fn string, content string) (err error) {
	var file *os.File
	if file, err = os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}
	defer file.Close()

	// var n int
	if _, err = file.WriteAt([]byte(content), 0); err != nil {
		return
	}

	return
}
