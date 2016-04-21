//go:generate ffjson $GOFILE
package fast_lem

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pquerna/ffjson/ffjson"
)

const FactSetDateFormat = `2006-01-02`
const DescriptionDateFormat = `2006/01/02`

func NewDescription(code, ticker, coupon, maturity string) (d *Description, err error) {
	d = &Description{
		Ticker:    ticker,
		IssueType: IssueTypeFromString(code),
	}
	if len(coupon) > 0 {
		d.Coupon, err = strconv.ParseFloat(coupon, 64)
		if err != nil {
			return
		}
	}
	if len(maturity) > 0 {
		d.Maturity, err = time.Parse(FactSetDateFormat, maturity)
		if err != nil {
			return
		}
	}
	return
}

func New(cusip, isin, sedol, ticker, entityID, issueTypeCode, coupon,
	maturity string) (s *Security) {
	desc, err := NewDescription(issueTypeCode, ticker, coupon, maturity)
	if err != nil {
		panic(err)
	}
	return &Security{
		LegalEntityID: entityID,
		CUSIP:         cusip,
		ISIN:          isin,
		SEDOL:         sedol,
		Ticker:        ticker,
		Description:   *desc,
	}
}

// ffjson: skip
type Description struct {
	IssueType IssueType
	Coupon    float64
	Maturity  time.Time
	Ticker    string
}

func (d Description) MarshalJSON() ([]byte, error) {
	var details []string
	if len(d.Ticker) > 0 {
		details = append(details, d.Ticker)
	}
	if d.Coupon != 0 {
		details = append(details, fmt.Sprintf("%0.02f%%", d.Coupon))
	}
	if !d.Maturity.IsZero() {
		details = append(details, d.Maturity.Format(DescriptionDateFormat))
	}
	if len(details) > 0 {
		return ffjson.Marshal(d.IssueType.String() + "  " + strings.Join(details, " "))
	}
	return ffjson.Marshal(d.IssueType.String())
}

// ffjson: nodecoder
type Security struct {
	LegalEntityID string      `json:"LegalEntityId,omitempty"`
	CUSIP         string      `json:"Cusip,omitempty"`
	ISIN          string      `json:",omitempty"`
	SEDOL         string      `json:"Sedol,omitempty"`
	Ticker        string      `json:",omitempty"`
	Description   Description `json:",omitempty"`
}

// ffjson: noencoder
type Request struct {
	Keys []string
}

// ffjson: nodecoder
type Response struct {
	Results map[string]*Security
}

func NewResponse() *Response {
	return &Response{
		Results: make(map[string]*Security),
	}
}
