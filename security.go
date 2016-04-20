//go:generate ffjson $GOFILE
package fast_lem

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func IssueTypeDesc(code string) (desc string, err error) {
	switch code {
	case "99":
		return "N/A", nil
	case "AB":
		return "Asset-Backed", nil
	case "AD":
		return "ADR/GDR", nil
	case "AG":
		return "Agency Bond", nil
	case "AI":
		return "Alternative Invt", nil
	case "BC":
		return "Convertible Bond", nil
	case "BD":
		return "Bond", nil
	case "CA":
		return "Cash/Repo/MM", nil
	case "CE":
		return "Closed-End Mutual Fund", nil
	case "CP":
		return "Convertible Preferred", nil
	case "DB":
		return "Debenture", nil
	case "DL":
		return "Dual Listing", nil
	case "DR":
		return "Derivative", nil
	case "EP":
		return "Equity (Pre-IPO)", nil
	case "EQ":
		return "Equity", nil
	case "ET":
		return "Exchange Traded Fund", nil
	case "FM":
		return "First Mortgage", nil
	case "FU":
		return "Future Agreement", nil
	case "FX":
		return "Fixed Income/Unclassified", nil
	case "ID":
		return "Index", nil
	case "LN":
		return "Loan", nil
	case "MB":
		return "Mortgage-Backed", nil
	case "MT":
		return "Medium Term Note", nil
	case "MU":
		return "Municipal Bonds", nil
	case "NT":
		return "Note", nil
	case "OE":
		return "Open-End Mutual Fund", nil
	case "OP":
		return "Stock Option", nil
	case "PF":
		return "Preferred", nil
	case "PQ":
		return "Private Equity", nil
	case "PV":
		return "Private Placement", nil
	case "SH":
		return "Short Position", nil
	case "UI":
		return "Unit Invt Trust", nil
	case "UL":
		return "Treasury/Long-Term", nil
	case "US":
		return "Treasury/Short-Term", nil
	case "WT":
		return "Warrant/Right", nil
	default:
		return "", errors.New("Undefined Issue Type '" + code + "'")
	}
}

func Description(code, ticker, coupon, maturity string) (d string, err error) {
	var itd string
	var rate float64
	var details []string
	itd, err = IssueTypeDesc(code)
	if err != nil {
		return
	}
	if len(ticker) > 0 {
		details = append(details, ticker)
	}
	if len(coupon) > 0 {
		rate, err = strconv.ParseFloat(coupon, 64)
		if err != nil {
			return
		}
		details = append(details, fmt.Sprintf("%0.02f%%", rate))
	}
	if len(maturity) > 0 {
		details = append(details, strings.Replace(maturity, "-", "/", -1))
	}
	if len(details) > 0 {
		return strings.Join([]string{itd, strings.Join(details, " ")}, "  "), nil
	}
	return itd, nil
}

func New(cusip, isin, sedol, ticker, entityID, issueTypeCode, coupon,
	maturity string) (s *Security) {
	desc, err := Description(issueTypeCode, ticker, coupon, maturity)
	if err != nil {
		desc = "Description not available"
	}
	return &Security{
		LegalEntityID: entityID,
		CUSIP:         cusip,
		ISIN:          isin,
		SEDOL:         sedol,
		Ticker:        ticker,
		Description:   desc,
	}
}

type Security struct {
	LegalEntityID string `json:"LegalEntityId,omitempty"`
	CUSIP         string `json:"Cusip,omitempty"`
	ISIN          string `json:",omitempty"`
	SEDOL         string `json:"Sedol,omitempty"`
	Ticker        string `json:",omitempty"`
	Description   string `json:",omitempty"`
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
