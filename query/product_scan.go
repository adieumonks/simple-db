package query

var _ Scan = (*ProductScan)(nil)

type ProductScan struct {
	s1 Scan
	s2 Scan
}

func NewProductScan(s1, s2 Scan) (*ProductScan, error) {

	ps := &ProductScan{s1: s1, s2: s2}
	if err := ps.BeforeFirst(); err != nil {
		return nil, err
	}
	return ps, nil
}

func (ps *ProductScan) BeforeFirst() error {
	if err := ps.s1.BeforeFirst(); err != nil {
		return err
	}
	if _, err := ps.s1.Next(); err != nil {
		return err
	}
	if err := ps.s2.BeforeFirst(); err != nil {
		return err
	}
	return nil
}

func (ps *ProductScan) Next() (bool, error) {
	if ok, err := ps.s2.Next(); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}

	if err := ps.s2.BeforeFirst(); err != nil {
		return false, err
	}

	ok1, err := ps.s1.Next()
	if err != nil {
		return false, err
	}
	ok2, err := ps.s2.Next()
	if err != nil {
		return false, err
	}
	return ok1 && ok2, nil
}

func (ps *ProductScan) GetInt(fieldName string) (int32, error) {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetInt(fieldName)
	}
	return ps.s2.GetInt(fieldName)
}

func (ps *ProductScan) GetString(fieldName string) (string, error) {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetString(fieldName)
	}
	return ps.s2.GetString(fieldName)
}

func (ps *ProductScan) GetVal(fieldName string) (*Constant, error) {
	if ps.s1.HasField(fieldName) {
		return ps.s1.GetVal(fieldName)
	}
	return ps.s2.GetVal(fieldName)
}

func (ps *ProductScan) HasField(fieldName string) bool {
	return ps.s1.HasField(fieldName) || ps.s2.HasField(fieldName)
}

func (ps *ProductScan) Close() {
	ps.s1.Close()
	ps.s2.Close()
}
