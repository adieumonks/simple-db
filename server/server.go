package server

import (
	"fmt"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

const (
	LOG_FILE = "simpledb.log"
)

type SimpleDB struct {
	fm *file.FileManager
	lm *log.LogManager
	bm *buffer.BufferManager
}

func NewSimpleDB(dirname string, blockSize, buffferSize int32) (*SimpleDB, error) {
	fm, err := file.NewFileManager(dirname, blockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create new file manager: %w", err)
	}

	lm, err := log.NewLogManager(fm, LOG_FILE)
	if err != nil {
		return nil, fmt.Errorf("failed to create new log manager: %w", err)
	}

	bm := buffer.NewBufferManager(fm, lm, buffferSize)

	return &SimpleDB{
		fm: fm,
		lm: lm,
		bm: bm,
	}, nil
}

func (db *SimpleDB) FileManager() *file.FileManager {
	return db.fm
}

func (db *SimpleDB) LogManager() *log.LogManager {
	return db.lm
}

func (db *SimpleDB) BufferManager() *buffer.BufferManager {
	return db.bm
}
