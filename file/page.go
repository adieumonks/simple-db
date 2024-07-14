package file

import (
	"encoding/binary"
	"unicode/utf16"
)

type Page struct {
	buffer []byte
}

const (
	Int32Bytes int32 = 4
	utf16Size  int32 = 2
)

func NewPage(blockSize int32) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

func (p *Page) GetInt(offset int32) int32 {
	data := p.buffer[offset : offset+Int32Bytes]
	val := binary.LittleEndian.Uint32(data)
	return int32(val)
}

func (p *Page) SetInt(offset int32, n int32) {
	data := make([]byte, Int32Bytes)
	binary.LittleEndian.PutUint32(data, uint32(n))
	copy(p.buffer[offset:offset+Int32Bytes], data)
}

func (p *Page) GetBytes(offset int32) []byte {
	length := p.GetInt(offset)
	return p.buffer[offset+Int32Bytes : offset+Int32Bytes+length]
}

func (p *Page) SetBytes(offset int32, b []byte) {
	p.SetInt(offset, int32(len(b)))
	copy(p.buffer[offset+Int32Bytes:offset+Int32Bytes+int32(len(b))], b)
}

func (p *Page) GetString(offset int32) string {
	length := p.GetInt(offset) / utf16Size

	runes := make([]uint16, length)
	for i := range length {
		runes[i] = p.getUint16(offset + Int32Bytes + int32(i)*utf16Size)
	}

	return string(utf16.Decode(runes))
}

func (p *Page) SetString(offset int32, s string) {
	runes := utf16.Encode([]rune(s))

	p.SetInt(offset, int32(int32(len(runes))*utf16Size))

	for i, r := range runes {
		p.setUint16(offset+Int32Bytes+int32(i)*utf16Size, r)
	}
}

func (p *Page) getUint16(offset int32) uint16 {
	data := p.buffer[offset : offset+utf16Size]
	return binary.LittleEndian.Uint16(data)
}

func (p *Page) setUint16(offset int32, n uint16) {
	data := make([]byte, utf16Size)
	binary.LittleEndian.PutUint16(data, n)
	copy(p.buffer[offset:offset+utf16Size], data)
}

func MaxLength(length int32) int32 {
	return Int32Bytes + length*utf16Size
}
