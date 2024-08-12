package query

import (
	"strings"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{}
}

func NewPredicateFromTerm(t *Term) *Predicate {
	return &Predicate{
		terms: []*Term{t},
	}
}

func (p *Predicate) ConjoinWith(other *Predicate) {
	p.terms = append(p.terms, other.terms...)
}

func (p *Predicate) IsSatisfied(scan Scan) (bool, error) {
	for _, term := range p.terms {
		isSatisfied, err := term.IsSatisfied(scan)
		if err != nil {
			return false, err
		}
		if !isSatisfied {
			return false, nil
		}
	}
	return true, nil
}

func (p *Predicate) ReductionFactor(plan Plan) int32 {
	var factor int32
	for _, term := range p.terms {
		factor *= term.ReductionFactor(plan)
	}
	return factor
}

func (p *Predicate) EquatesWithConstant(fieldName string) *Constant {
	for _, term := range p.terms {
		if constant := term.EquatesWithConstant(fieldName); constant != nil {
			return constant
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		if otherFieldName := term.EquatesWithField(fieldName); otherFieldName != "" {
			return otherFieldName
		}
	}
	return ""
}

func (p *Predicate) String() string {
	var terms []string
	for _, term := range p.terms {
		terms = append(terms, term.String())
	}
	return strings.Join(terms, " and ")
}
