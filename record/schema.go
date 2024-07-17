package record

type FieldType int32

const (
	INTEGER FieldType = iota
	STRING
)

type FieldInfo struct {
	fieldType FieldType
	length    int32
}

type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

func (s *Schema) AddField(fieldName string, fieldType FieldType, length int32) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = FieldInfo{fieldType, length}
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, INTEGER, 0)
}

func (s *Schema) AddStringField(fieldName string, length int32) {
	s.AddField(fieldName, STRING, length)
}

func (s *Schema) Add(fieldName string, sch Schema) {
	fieldType := sch.Type(fieldName)
	length := sch.Length(fieldName)
	s.AddField(fieldName, fieldType, length)
}

func (s *Schema) AddAll(sch Schema) {
	for _, fieldName := range sch.Fields() {
		s.Add(fieldName, sch)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fieldName string) bool {
	_, ok := s.info[fieldName]
	return ok
}

func (s *Schema) Type(fieldName string) FieldType {
	return s.info[fieldName].fieldType
}

func (s *Schema) Length(fieldName string) int32 {
	return s.info[fieldName].length
}
