package fast_lem

import (
	"encoding/csv"
	"io"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

var (
	BadQuoteEscape  = []byte(`\"`)
	GoodQuoteEscape = []byte(`""`)
)

// QuoteEscaper implements transform.Transformer, replacing \" with ""
// This fixes non-standard escaping in some delimited text inputs
type QuoteEscaper struct {
	transform.NopResetter
}

// NewReader handles the transformations necessary to process a FactSet EDM data file
// with Go's csv library
func NewReader(source io.Reader) *csv.Reader {
	d := NewDecoder()
	r := csv.NewReader(d.Reader(source))
	r.Comma = '|'
	r.LazyQuotes = true
	return r
}

// NewDecoder returns a decoder to handle character conversion and non-standard escaping of
// double quotation marks in CSV
func NewDecoder() *encoding.Decoder {
	return &encoding.Decoder{Transformer: transform.Chain(charmap.Windows1252.NewDecoder(), &QuoteEscaper{})}
}

// Transform replaces the sequence `\"` with `""`
func (q QuoteEscaper) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	for i, c := range src {
		if nDst >= len(dst) {
			err = transform.ErrShortDst
			break
		}
		if c != 0x5c {
			dst[nDst] = c
			nDst++
			nSrc = i + 1
			continue
		}
		// c is a backslash
		if (i < len(src)-1) && src[i+1] == 0x22 {
			dst[nDst] = 0x22
			nDst++
			nSrc = i + 1
			continue
		}
		dst[nDst] = c
		nDst++
		nSrc = i + 1
		continue
	}
	return nDst, nSrc, err
}
