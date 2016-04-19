package fast_lem

import "bytes"

var (
	BadQuoteEscape  = []byte(`\"`)
	GoodQuoteEscape = []byte(`""`)
)

// QuoteEscaper implements transform.Transformer, replacing \" with ""
// This fixes non-standard escaping in some delimited text inputs
type QuoteEscaper struct{}

func (q QuoteEscaper) Transform(dst, src []byte, atEOF bool) (
	nDst, nSrc int, err error) {
	dst = bytes.Replace(src, BadQuoteEscape, GoodQuoteEscape, -1)
	nDst = len(dst)
	nSrc = len(src)
	err = nil
	return
}

func (q QuoteEscaper) Reset() {
	return
}
