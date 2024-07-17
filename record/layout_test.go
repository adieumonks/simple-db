package record_test

import (
	"testing"

	"github.com/adieumonks/simple-db/record"
)

func TestLayout(t *testing.T) {
	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(sch)
	for _, fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		t.Logf("%s has offset %d\n", fieldName, offset)
	}
}
