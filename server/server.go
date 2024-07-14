package server

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
)

type SimpleDB struct {
	fileManager *file.FileManager
}

func NewSimpleDB(dirname string, blockSize int32) (*SimpleDB, error) {
	fileManager, err := file.NewFileManager(dirname, blockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create new file manager: %w", err)
	}
	return &SimpleDB{
		fileManager: fileManager,
	}, nil
}

func (db *SimpleDB) FileManager() *file.FileManager {
	return db.fileManager
}
