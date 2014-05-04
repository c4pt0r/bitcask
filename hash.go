package bitcask

import (
	"sync"
)

type HashTable struct {
	l  sync.RWMutex
	m  map[string]*DocRecHdr
	db *DbFile
}

func NewHashTable(filename string) *HashTable {
    ret := new(HashTable)
    db, err := OpenDb(filename)
    if err != nil {
        return nil
    }
    ret.Load(db)
    return ret
}

func (self *HashTable) Load(db *DbFile) {
	self.l.Lock()
	defer self.l.Unlock()
	m := make(map[string]*DocRecHdr)
	for db.HasNext() {
		recHdr := db.curRecHdr()
		key := db.ReadRecordKey(recHdr)
		if v, exists := m[string(key)]; (exists && v.ts < recHdr.ts) || !exists {
			m[string(key)] = recHdr
		}
		db.Next()
	}
	self.m = m
    self.db = db
}

func (self *HashTable) Get(key string) map[string][]byte {
	self.l.RLock()
	defer self.l.RUnlock()
	if hdr, b := self.m[key]; b {
		rec := self.db.ReadRecord(hdr)
		return rec.doc
	}
	return nil
}

func (self *HashTable) Set(key string, value map[string][]byte) error {
	self.l.Lock()
	defer self.l.Unlock()
	doc := NewDoc([]byte(key), value)
	hdr, err := self.db.AppendRecord(doc)
	if err != nil {
		return err
	}
	self.m[key] = hdr
	return nil
}
