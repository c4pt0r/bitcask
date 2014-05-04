package bitcask

import (
	"log"
	"testing"
    "fmt"
)
/*
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

func TestHash(t *testing.T) {
    h := NewHashTable("/tmp/tmpdb")
    m := make(map[string][]byte)
    m["field1"] = []byte("value1")
    m["field2"] = []byte("value1")
    h.Set("my key", m)

    m1 := h.Get("my key")
    if m1 != nil {
        log.Println("success", string(m1["field1"]))
    }
}
*/
func TestHash(t *testing.T) {
    h := NewHashTable("/tmp/tmpdb")
    m := make(map[string][]byte)
    m["field1"] = []byte("value1")
    m["field2"] = []byte("value1")
    for i := 0; i < 100; i++ {
        k := fmt.Sprintf("key_%d", i)
        h.Set(k, m)
    }
    log.Println(len(h.m))
    h.db.Compact()
}
