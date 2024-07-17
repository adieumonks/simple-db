package query

type Constant struct {
	ival *int32
	sval *string
}

func NewConstantFromInt(ival int32) *Constant {
	return &Constant{ival: &ival}
}

func NewConstantFromString(sval string) *Constant {
	return &Constant{sval: &sval}
}

func (c *Constant) AsInt() int32 {
	return *c.ival
}

func (c *Constant) AsString() string {
	return *c.sval
}

func (c *Constant) String() string {
	if c.ival != nil {
		return string(*c.ival)
	}
	return *c.sval
}
