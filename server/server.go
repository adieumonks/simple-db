package server

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

const (
	LOG_FILE = "simpledb.log"
)

type SimpleDB struct {
	fm *file.FileManager
	lm *log.LogManager
}

func NewSimpleDB(dirname string, blockSize int32) (*SimpleDB, error) {
	fm, err := file.NewFileManager(dirname, blockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create new file manager: %w", err)
	}

	lm, err := log.NewLogManager(fm, LOG_FILE)
	if err != nil {
		return nil, fmt.Errorf("failed to create new log manager: %w", err)
	}

	return &SimpleDB{
		fm: fm,
		lm: lm,
	}, nil
}

func (db *SimpleDB) FileManager() *file.FileManager {
	return db.fm
}

func (db *SimpleDB) LogManager() *log.LogManager {
	return db.lm
}
