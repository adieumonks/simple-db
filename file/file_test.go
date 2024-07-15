package file_test

import (
	"testing"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/server"
)

func TestFile(t *testing.T) {
	db, err := server.NewSimpleDB("filetest", 400, 8)
	if err != nil {
		t.Fatalf("failed to create new simple db: %v", err)
	}

	fm := db.FileManager()

	var pos1 int32 = 88
	p1 := file.NewPage(fm.BlockSize())
	strVal := "abcdefghijklm"
	p1.SetString(pos1, strVal)

	size := file.MaxLength(int32(len(strVal)))
	pos2 := pos1 + size
	intVal := int32(345)
	p1.SetInt(pos2, intVal)

	block := file.NewBlockID("testfile", 2)
	err = fm.Write(block, p1)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	p2 := file.NewPage(fm.BlockSize())
	err = fm.Read(block, p2)
	if err != nil {
		t.Fatalf("failed to read block: %v", err)
	}

	t.Logf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	t.Logf("offset %d contains %s\n", pos1, p2.GetString(pos1))

	if p2.GetInt(pos2) != intVal {
		t.Errorf("expected %d, got %d", intVal, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != strVal {
		t.Errorf("expected %s, got %s", strVal, p2.GetString(pos1))
	}
}
