package record

import (
	"github.com/adieumonks/simple-db/file"
)

type Layout struct {
	schema   *Schema
	offsets  map[string]int32
	slotSize int32
}

func NewLayoutFromSchema(schema *Schema) *Layout {
	l := &Layout{
		schema:  schema,
		offsets: make(map[string]int32),
	}
	pos := file.Int32Bytes
	for _, fieldName := range schema.Fields() {
		l.offsets[fieldName] = pos
		pos += l.LengthInBytes(fieldName)
	}
	l.slotSize = pos
	return l
}

func NewLayout(schema *Schema, offsets map[string]int32, slotSize int32) *Layout {
	return &Layout{schema, offsets, slotSize}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int32 {
	return l.offsets[fieldName]
}

func (l *Layout) SlotSize() int32 {
	return l.slotSize
}

func (l *Layout) LengthInBytes(fieldName string) int32 {
	fieldType := l.schema.Type(fieldName)
	if fieldType == INTEGER {
		return file.Int32Bytes
	} else {
		return file.MaxLength(l.schema.Length(fieldName))
	}
}
