package fast_lem

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
)

func TestQuoteEscaperTranform(t *testing.T) {
	e := &QuoteEscaper{}
	src := []byte(`"FDS010000"|"USFDS0100006"|""|""|""|"000XT9-E"|"TOYS \"R\" US INC  AB REV"|"US"|"LN"|""|2009-06-24|2010-07-21|""|"USD"|"US81"||2010-07-21`)
	want := []byte(`"FDS010000"|"USFDS0100006"|""|""|""|"000XT9-E"|"TOYS ""R"" US INC  AB REV"|"US"|"LN"|""|2009-06-24|2010-07-21|""|"USD"|"US81"||2010-07-21`)
	var dst = make([]byte, len(src))
	_, _, err := e.Transform(dst, src, false)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(dst, want) {
		t.Errorf("Got '%s', want '%s'", string(dst), string(want))
	}
}

func TestReader(t *testing.T) {
	f, err := os.Open("test_files/edm.psv")
	if err != nil {
		t.Error(err)
	}
	d := NewDecoder()
	escapedUTF8 := d.Reader(f)
	r := csv.NewReader(escapedUTF8)
	r.Comma = '|'
	r.LazyQuotes = true
	var row []string
	row, err = r.Read()
	if len(row) != 17 {
		fmt.Printf("%+v\n", row)
		t.Errorf("Expected 17 elements in the row, got %d", len(row))
	}
}
