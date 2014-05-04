package bitcask
import (
    "encoding/binary"
    "bytes"
)

func Uint16ToBytes(i uint16) []byte {
    data := make([]byte, 2)
    binary.LittleEndian.PutUint16(data, i)
    return data
}

func Uint32ToBytes(i uint32) []byte {
    data := make([]byte, 4)
    binary.LittleEndian.PutUint32(data, i)
    return data
}

func Uint64ToBytes(i uint64) []byte {
    data := make([]byte, 8)
    binary.LittleEndian.PutUint64(data, i)
    return data
}

func MapToBytes(m map[string][]byte) []byte {
    var buf []byte
    for field, v := range m {
        fieldSz := make([]byte, 2)
        valSz := make([]byte, 4)
        binary.LittleEndian.PutUint16(fieldSz, uint16(len(field)))
        binary.LittleEndian.PutUint32(valSz, uint32(len(v)))
        buf = append(buf, fieldSz...)
        buf = append(buf, field...)
        buf = append(buf, valSz...)
        buf = append(buf, v...)
    }
    return buf
}

func BytesToMap(b []byte) map[string][]byte {
    r := make(map[string][]byte)
    buf := bytes.NewBuffer(b)
    for buf.Len() > 0 {
        fieldSzBuf := make([]byte, 2)
        valSzBuf := make([]byte, 4)

        buf.Read(fieldSzBuf)
        fieldSz := binary.LittleEndian.Uint16(fieldSzBuf)
        field := make([]byte, fieldSz)
        buf.Read(field)

        buf.Read(valSzBuf)
        valSz := binary.LittleEndian.Uint16(valSzBuf)
        val := make([]byte, valSz)
        buf.Read(val)

        r[string(field)] = val
    }
    return r
}
