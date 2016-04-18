package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"

	"github.com/nycmonkey/fast_lem"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

var (
	source string
)

func init() {
	flag.StringVar(&source, "source", "data.csv", "path to the source data")
	flag.Parse()
}

const (
	ColumnCUSIP = iota // 0
	ColumnISIN
	ColumnSEDOL
	ColumnTicker
	_ // FS_PERM_SEC_ID
	ColumnEntityID
	_ // SECURITY_NAME
	_ // ISO_COUNTRY
	ColumnIssueType
	_ // FDS_PRIMARY_MIC_EXCHANGE_CODE
	_ // INCEPTION_DATE
	_ // TERMINATION_DATE
	_ // CAP_GROUP
	_ // FDS_PRIMARY_ISO_CURRENCY
	_ // CIC_CODE
	ColumnCouponRate
	ColumnMaturityDate
)

func main() {
	data, err := os.Open(source)
	if err != nil {
		log.Fatalln(err)
	}
	defer data.Close()
	utf8 := transform.NewReader(data, charmap.Windows1252.NewDecoder())
	r := csv.NewReader(utf8)
	r.Comma = '|'
	r.LazyQuotes = true
	r.FieldsPerRecord = 17
	var row []string
	row, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}
	i := 0
	var security *fast_lem.Security
	for {
		i++
		row, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		security = fast_lem.EDMSecurityRecord{
			CUSIP:         row[ColumnCUSIP],
			ISIN:          row[ColumnISIN],
			SEDOL:         row[ColumnSEDOL],
			Ticker:        row[ColumnTicker],
			EntityID:      row[ColumnEntityID],
			IssueTypeCode: row[ColumnIssueType],
			CouponRate:    row[ColumnCouponRate],
			MaturityDate:  row[ColumnMaturityDate],
		}.Transform()
		var buf []byte
		buf, err = json.Marshal(security)
		if err != nil {
			log.Fatalln(err)
		}
		os.Stdout.Write(buf)
		os.Stdout.Write([]byte("\r\n"))
	}
	return
}
