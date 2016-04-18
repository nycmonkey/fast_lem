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

type EDMSecurityRecord struct {
	CUSIP, ISIN, SEDOL, Ticker, EntityID, IssueTypeCode, CouponRate, MaturityDate string
}

func (r EDMSecurityRecord) Description() (desc string, err error) {
	var itd string
	itd, err = IssueTypeDesc(r.IssueTypeCode)
	if err != nil {
		return
	}
	var details []string
	if len(r.Ticker) > 0 {
		details = append(details, r.Ticker)
	}
	if len(r.CouponRate) > 0 {
		rate, ohShit := strconv.ParseFloat(r.CouponRate, 32)
		if ohShit != nil {
			panic(ohShit)
		}
		details = append(details, fmt.Sprintf("%0.02f%%", rate))
	}
	if len(r.MaturityDate) > 0 {
		details = append(details, r.MaturityDate)
	}
	if len(details) > 0 {
		return strings.Join([]string{itd, strings.Join(details, " ")}, "  "), nil
	}
	return itd, nil
}

func (r EDMSecurityRecord) Transform() *Security {
	desc, err := r.Description()
	if err != nil {
		desc = "Description not available"
	}
	return &Security{
		LegalEntityID: r.EntityID,
		CUSIP:         r.CUSIP,
		ISIN:          r.ISIN,
		SEDOL:         r.SEDOL,
		Ticker:        r.Ticker,
		Description:   desc,
	}
}

type Security struct {
	LegalEntityID string `json:",omitempty"`
	CUSIP         string `json:",omitempty"`
	ISIN          string `json:",omitempty"`
	SEDOL         string `json:",omitempty"`
	Ticker        string `json:",omitempty"`
	Description   string `json:",omitempty"`
}
