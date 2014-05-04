package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type DocRecHdr struct {
	crc    uint32 // 4bytes offset : 0
	ts     int64  // 8bytes offset : 4
	kSz    uint16 // 2bytes offset : 12
	docSz  uint32 // 4bytes offset : 14, total 18 + 8 bytes
	offset int64  // 8bytes
}

type DocRec struct {
	DocRecHdr
	key []byte
	doc map[string][]byte
}

func NewDoc(key []byte, doc map[string][]byte) *DocRec {
	r := new(DocRec)
	buf := MapToBytes(doc)
	r.DocRecHdr = DocRecHdr{
		crc:    crc32.ChecksumIEEE(buf),
		ts:     time.Now().Unix(),
		kSz:    uint16(len(key)),
		docSz:  uint32(len(buf)),
		offset: -1,
	}
	r.key = key
	r.doc = doc
	return r
}

type DbFile struct {
	fp       *os.File
	cur      int64
	sz       int64
	filename string
	l        sync.Mutex
}

func OpenDb(filename string) (*DbFile, error) {
	ret := new(DbFile)
	ret.filename = filename
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	ret.fp = fp
	fi, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, err
	}
	ret.sz = fi.Size()
	ret.l = sync.Mutex{}
	ret.cur = 0
	return ret, nil
}

func DbFromFp(fp *os.File) (*DbFile, error) {
	ret := new(DbFile)
	ret.fp = fp
	fi, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, err
	}
	ret.sz = fi.Size()
	ret.l = sync.Mutex{}
	ret.cur = 0
	return ret, nil
}

func (self *DbFile) AppendRecord(rec *DocRec) (*DocRecHdr, error) {
	self.l.Lock()
	defer self.l.Unlock()
	hdr := make([]byte, 26)
	pos := 0
	fi, _ := self.fp.Stat()
	rec.offset = fi.Size()

	binary.LittleEndian.PutUint32(hdr[pos:], rec.crc)
	pos += 4
	binary.LittleEndian.PutUint64(hdr[pos:], uint64(rec.ts))
	pos += 8
	binary.LittleEndian.PutUint16(hdr[pos:], rec.kSz)
	pos += 2
	binary.LittleEndian.PutUint32(hdr[pos:], rec.docSz)
	pos += 4
	binary.LittleEndian.PutUint64(hdr[pos:], uint64(rec.offset))

	self.fp.Write(hdr)
	self.fp.Write(rec.key)
	self.fp.Write(MapToBytes(rec.doc))

	fi, _ = self.fp.Stat()
	self.sz = fi.Size()
	return &rec.DocRecHdr, nil
}

func (self *DbFile) Close() {
	self.l.Lock()
	defer self.l.Unlock()
	self.fp.Close()
}

func (self *DbFile) curRecHdr() *DocRecHdr {
	rec := new(DocRecHdr)
	hdr := make([]byte, 26)
	self.fp.ReadAt(hdr, self.cur)
	pos := 0
	rec.crc = binary.LittleEndian.Uint32(hdr[pos:])
	pos += 4
	rec.ts = int64(binary.LittleEndian.Uint64(hdr[pos:]))
	pos += 8
	rec.kSz = binary.LittleEndian.Uint16(hdr[pos:])
	pos += 2
	rec.docSz = binary.LittleEndian.Uint32(hdr[pos:])
	pos += 4
	rec.offset = int64(binary.LittleEndian.Uint64(hdr[pos:]))
	return rec
}

func (self *DbFile) ReadRecord(hdr *DocRecHdr) *DocRec {
	self.l.Lock()
	defer self.l.Unlock()
	off := hdr.offset
	rec := &DocRec{
		DocRecHdr: *hdr,
		key:       nil,
		doc:       nil,
	}
	keyBuf := make([]byte, int(hdr.kSz))
	self.fp.ReadAt(keyBuf, off+26)
	valBuf := make([]byte, int(hdr.docSz))
	self.fp.ReadAt(valBuf, off+26+int64(hdr.kSz))
	rec.key = append(rec.key, keyBuf...)
	rec.doc = BytesToMap(valBuf)
	return rec
}

func (self *DbFile) ReadRecordKey(hdr *DocRecHdr) []byte {
	self.l.Lock()
	defer self.l.Unlock()
	off := hdr.offset
	keyBuf := make([]byte, int(hdr.kSz))
	self.fp.ReadAt(keyBuf, off+26)
	return keyBuf
}

func (self *DbFile) ReadCurRecord() *DocRec {
	self.l.Lock()
	hdr := self.curRecHdr()
	self.l.Unlock()
	return self.ReadRecord(hdr)
}

func (self *DbFile) move(i int64) {
	self.fp.Seek(i, 0)
	atomic.StoreInt64(&self.cur, i)
}

func (self *DbFile) Reset() (int64, *DocRecHdr) {
	self.l.Lock()
	defer self.l.Unlock()
	self.move(0)
	return self.cur, self.curRecHdr()
}

func (self *DbFile) HasNext() bool {
	self.l.Lock()
	defer self.l.Unlock()
	curHdr := self.curRecHdr()
	if self.cur+int64(uint32(curHdr.kSz)+curHdr.docSz+26) > self.sz {
		return false
	}
	return true
}

func (self *DbFile) Next() (int64, *DocRecHdr) {
	self.l.Lock()
	defer self.l.Unlock()
	curHdr := self.curRecHdr()
	self.move(self.cur + int64(uint32(curHdr.kSz)+curHdr.docSz+26))
	return self.cur, self.curRecHdr()
}

func (self *DbFile) EOF() bool {
	if self.cur > self.sz {
		return true
	}
	return false
}

// compact the old file and return new instance
func (self *DbFile) Compact() (*DbFile, error) {
	fname := self.filename
	fp, err := os.OpenFile(fname, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	db, _ := DbFromFp(fp)
	m := make(map[string]*DocRec)
	for db.HasNext() {
		rec := db.ReadCurRecord()
		if v, exists := m[string(rec.key)]; (exists && v.ts < rec.ts) || !exists {
			m[string(rec.key)] = rec
		}
		db.Next()
	}

	newFp, err := os.OpenFile(fname+".new", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	newDb, _ := DbFromFp(newFp)
	for _, v := range m {
		newDb.AppendRecord(v)
	}
	return newDb, nil
}
