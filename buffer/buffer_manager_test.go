package buffer_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/server"
)

func TestBufferManager(t *testing.T) {
	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "buffertest"), 400, 3)
	if err != nil {
		t.Fatalf("failed to create new simple db: %v", err)
	}

	// init
	fm := db.FileManager()
	for i := 0; i < 4; i++ {
		_, err := fm.Append("testfile")
		if err != nil {
			t.Fatalf("failed to append block: %v", err)
		}
	}

	bm := db.BufferManager()

	buff := make([]*buffer.Buffer, 6)
	buff[0], _ = bm.Pin(file.NewBlockID("testfile", 0))
	buff[1], _ = bm.Pin(file.NewBlockID("testfile", 1))
	buff[2], _ = bm.Pin(file.NewBlockID("testfile", 2))
	bm.Unpin(buff[1])
	buff[1] = nil
	buff[3], _ = bm.Pin(file.NewBlockID("testfile", 0)) // block 0 pinned twice
	buff[4], _ = bm.Pin(file.NewBlockID("testfile", 1)) // block 1 repinned
	t.Logf("available buffers: %d", bm.Available())

	t.Log("attempting to pin block 3...")
	buff[5], err = bm.Pin(file.NewBlockID("testfile", 3))
	if err != nil {
		if err != buffer.ErrBufferAbort {
			t.Fatalf("expected buffer abort error, got %v", err)
		}
		t.Log("buffer abort error received")
	}

	bm.Unpin(buff[2])
	buff[2] = nil
	buff[5], _ = bm.Pin(file.NewBlockID("testfile", 3)) // now this works

	t.Log("final buffer allocation")
	for i, b := range buff {
		if b != nil {
			t.Logf("buffer[%d]: pinned to block %v", i, b.Block())
		}
	}
}
