package log_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
	"github.com/adieumonks/simple-db/server"
)

func TestLog(t *testing.T) {
	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "logtest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create new simple db: %v", err)
	}

	lm := db.LogManager()

	printLogRecords(t, lm, "The initial empty log file")
	t.Log("done")

	createRecords(t, lm, 1, 35)
	printLogRecords(t, lm, "The log file now has these records:")
}

func printLogRecords(t *testing.T, lm *log.LogManager, msg string) {
	t.Log(msg)

	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("failed to create log iterator: %v", err)
	}

	for iter.HasNext() {
		record := iter.Next()
		p := file.NewPageFromBytes(record)
		s := p.GetString(0)
		npos := file.MaxLength(int32(len(s)))
		val := p.GetInt(npos)
		t.Logf("[%s, %d]\n", s, val)
	}
}

func createRecords(t *testing.T, lm *log.LogManager, start, end int32) {
	t.Log("Creating records: ")
	for i := start; i <= end; i++ {
		s := fmt.Sprintf("record %d", i)
		rec := createLogRecord(s, i+100)
		lsn, err := lm.Append(rec)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		t.Logf("%d ", lsn)
	}
}

func createLogRecord(s string, n int32) []byte {
	var spos int32 = 0
	npos := spos + file.MaxLength(int32(len(s)))
	b := make([]byte, npos+file.Int32Bytes)
	p := file.NewPageFromBytes(b)
	p.SetString(spos, s)
	p.SetInt(npos, n)
	return b
}
