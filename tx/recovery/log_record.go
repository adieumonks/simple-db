package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
)

type LogRecordType int32

const (
	CHECKPOINT LogRecordType = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int32
	Undo(tx Transaction)
}

func NewLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageFromBytes(bytes)
	switch LogRecordType(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckpointRecord(), nil
	case START:
		return NewStartRecordFrom(p), nil
	case COMMIT:
		return NewCommitRecordFrom(p), nil
	case ROLLBACK:
		return NewRollbackRecordFrom(p), nil
	case SETINT:
		return NewSetIntRecordFrom(p), nil
	case SETSTRING:
		return NewSetStringRecordFrom(p), nil
	default:
		return nil, fmt.Errorf("invalid log record type %v", p.GetInt(0))
	}
}
