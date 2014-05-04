package bitcask

import (
	"log"
	"testing"
)

func TestDbFile(t *testing.T) {
	dbFile, err := OpenDb("/tmp/tmpdb")
	if err != nil {
		t.Error(err)
	}
	m := make(map[string][]byte)
	m["hello"] = []byte("world")
	rec := NewDoc([]byte("key"), m)
	dbFile.AppendRecord(rec)

	rec2 := NewDoc([]byte("key2"), m)
	dbFile.AppendRecord(rec2)

	// move to header
	dbFile.Reset()

	for dbFile.HasNext() {
		rec := dbFile.ReadCurRecord()
		log.Println(rec)
		dbFile.Next()
	}

	newDb, _ := dbFile.Compact()
	log.Println("new db compacted")

	for newDb.HasNext() {
		rec := newDb.ReadCurRecord()
		log.Println(rec)
		newDb.Next()
	}
}

func TestBMap(t *testing.T) {
	m := make(map[string][]byte)
	m["hello"] = []byte("world")
	m["hello2"] = []byte("world2")
	log.Println("map to bytes", MapToBytes(m))

	b := MapToBytes(m)
	mm := BytesToMap(b)
	log.Println(mm)
}
