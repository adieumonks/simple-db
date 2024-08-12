package server

import (
	"fmt"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/tx"
)

const (
	BLOCK_SIZE  = 400
	BUFFER_SIZE = 8
	LOG_FILE    = "simpledb.log"
)

type SimpleDB struct {
	fm      *file.FileManager
	lm      *log.LogManager
	bm      *buffer.BufferManager
	mdm     *metadata.MetadataManager
	planner *plan.Planner
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

func NewSimpleDBWithMetadata(dirname string) (*SimpleDB, error) {
	db, err := NewSimpleDB(dirname, BLOCK_SIZE, BUFFER_SIZE)
	if err != nil {
		return nil, err
	}

	tx, err := db.NewTransaction()
	if err != nil {
		return nil, err
	}

	isNew := db.fm.IsNew()
	if isNew {
		fmt.Println("creating new database")
	} else {
		fmt.Println("recovering existing database")
		if err := tx.Recover(); err != nil {
			return nil, err
		}
	}

	mdm, err := metadata.NewMetadataManager(isNew, tx)
	if err != nil {
		return nil, err
	}

	db.mdm = mdm

	qp := plan.NewBasicQueryPlanner(mdm)
	up := plan.NewBasicUpdatePlanner(mdm)
	db.planner = plan.NewPlanner(qp, up)

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *SimpleDB) NewTransaction() (*tx.Transaction, error) {
	return tx.NewTransaction(db.fm, db.lm, db.bm)
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

func (db *SimpleDB) MetadataManager() *metadata.MetadataManager {
	return db.mdm
}

func (db *SimpleDB) Planner() *plan.Planner {
	return db.planner
}
