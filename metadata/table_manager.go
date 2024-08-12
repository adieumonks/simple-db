package metadata

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

const (
	MAX_NAME = 16
)

type TableManager struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) (*TableManager, error) {
	tm := &TableManager{}
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	tm.tcatLayout = record.NewLayoutFromSchema(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tm.fcatLayout = record.NewLayoutFromSchema(fcatSchema)

	if isNew {
		err := tm.CreateTable("tblcat", tcatSchema, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
		err = tm.CreateTable("fldcat", fcatSchema, tx)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	return tm, nil
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayoutFromSchema(schema)
	tcat, err := query.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	err = tcat.Insert()
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}
	err = tcat.SetString("tblname", tableName)
	if err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}
	err = tcat.SetInt("slotsize", layout.SlotSize())
	if err != nil {
		return fmt.Errorf("failed to set int: %w", err)
	}
	tcat.Close()

	fcat, err := query.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	for _, filedName := range schema.Fields() {
		err = fcat.Insert()
		if err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
		err = fcat.SetString("tblname", tableName)
		if err != nil {
			return fmt.Errorf("failed to set string: %w", err)
		}
		err = fcat.SetString("fldname", filedName)
		if err != nil {
			return fmt.Errorf("failed to set string: %w", err)
		}
		err = fcat.SetInt("type", int32(schema.Type(filedName)))
		if err != nil {
			return fmt.Errorf("failed to set int: %w", err)
		}
		err = fcat.SetInt("length", schema.Length(filedName))
		if err != nil {
			return fmt.Errorf("failed to set int: %w", err)
		}
		err = fcat.SetInt("offset", layout.Offset(filedName))
		if err != nil {
			return fmt.Errorf("failed to set int: %w", err)
		}
	}
	fcat.Close()
	return nil
}

func (tm *TableManager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	slotSize := int32(-1)
	tcat, err := query.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %w", err)
	}

	next, err := tcat.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get next: %w", err)
	}

	for next {
		tableNameAtRecord, err := tcat.GetString("tblname")
		if err != nil {
			return nil, fmt.Errorf("failed to get string: %w", err)
		}
		if tableNameAtRecord == tableName {
			slotSize, err = tcat.GetInt("slotsize")
			if err != nil {
				return nil, fmt.Errorf("failed to get int: %w", err)
			}
			break
		}
		next, err = tcat.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next: %w", err)
		}
	}
	tcat.Close()

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fcat, err := query.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %w", err)
	}
	next, err = fcat.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get next: %w", err)
	}
	for next {
		tableNameAtRecord, err := fcat.GetString("tblname")
		if err != nil {
			return nil, fmt.Errorf("failed to get string: %w", err)
		}
		if tableNameAtRecord == tableName {
			fieldName, err := fcat.GetString("fldname")
			if err != nil {
				return nil, fmt.Errorf("failed to get string: %w", err)
			}
			fieldType, err := fcat.GetInt("type")
			if err != nil {
				return nil, fmt.Errorf("failed to get int: %w", err)
			}
			length, err := fcat.GetInt("length")
			if err != nil {
				return nil, fmt.Errorf("failed to get int: %w", err)
			}
			offset, err := fcat.GetInt("offset")
			if err != nil {
				return nil, fmt.Errorf("failed to get int: %w", err)
			}
			schema.AddField(fieldName, record.FieldType(fieldType), length)
			offsets[fieldName] = offset
		}
		next, err = fcat.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next: %w", err)
		}
	}
	fcat.Close()
	return record.NewLayout(schema, offsets, slotSize), nil
}
