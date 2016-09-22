package cqlbuilder

type BatchBuilder struct {
	builders []CqlBuilder
}

func (b *BatchBuilder) Add(builder CqlBuilder) *BatchBuilder {
	b.builders = append(b.builders, builder)
	return b
}
