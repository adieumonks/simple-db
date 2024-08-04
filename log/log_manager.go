package log

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
)

type LogManager struct {
	fm           *file.FileManager
	logfile      string
	logPage      *file.Page
	currentBlock file.BlockID
	latestLSN    int32
	lastSavedLSN int32
}

func NewLogManager(fm *file.FileManager, logfile string) (*LogManager, error) {

	b := make([]byte, fm.BlockSize())
	logPage := file.NewPageFromBytes(b)

	lm := &LogManager{
		fm:      fm,
		logfile: logfile,
		logPage: logPage,
	}

	logSize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("failed to get log size: %w", err)
	}

	if logSize == 0 {
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("failed to append new block: %w", err)
		}
	} else {
		lm.currentBlock = file.NewBlockID(logfile, logSize-1)
		err := fm.Read(lm.currentBlock, logPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read log page: %w", err)
		}
	}

	return lm, nil
}

func (lm *LogManager) Flush(lsn int32) error {
	if lsn >= lm.lastSavedLSN {
		err := lm.flush()
		if err != nil {
			return fmt.Errorf("failed to flush log: %w", err)
		}
	}
	return nil
}

func (lm *LogManager) Iterator() (*LogIterator, error) {
	lm.flush()

	it, err := NewLogIterator(lm.fm, lm.currentBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to create log iterator: %w", err)

	}
	return it, nil
}

func (lm *LogManager) Append(rec []byte) (int32, error) {
	boundary := lm.logPage.GetInt(0)
	recSize := int32(len(rec))
	bytesneeded := recSize + file.Int32Bytes
	if boundary-bytesneeded < file.Int32Bytes {
		lm.flush()
		currentBlock, err := lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("failed to append new block: %w", err)
		}
		lm.currentBlock = currentBlock
		boundary = lm.logPage.GetInt(0)
	}

	recpos := boundary - bytesneeded
	lm.logPage.SetBytes(recpos, rec)
	lm.logPage.SetInt(0, recpos)
	lm.latestLSN++
	return lm.latestLSN, nil
}

func (lm *LogManager) appendNewBlock() (file.BlockID, error) {
	block, err := lm.fm.Append(lm.logfile)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("failed to append new block: %w", err)
	}
	lm.logPage.SetInt(0, lm.fm.BlockSize())
	if err := lm.fm.Write(block, lm.logPage); err != nil {
		return file.BlockID{}, err
	}
	return block, nil
}

func (lm *LogManager) flush() error {
	err := lm.fm.Write(lm.currentBlock, lm.logPage)
	if err != nil {
		return fmt.Errorf("failed to write log page: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}
