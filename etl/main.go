package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/nycmonkey/fast_lem"
)

var (
	source      string
	dbfile      string
	wg          = new(sync.WaitGroup)
	storage     fast_lem.Storage
	recordCount int
)

func init() {
	flag.StringVar(&source, "source", "data.csv", "path to the source data")
	flag.StringVar(&dbfile, "output", "../db/lem.db",
		"path to a boltdb database where the data will be stored")
	flag.Parse()
}

const (
	bucketName         = `SecuritiesByCusip`
	isinMappingBucket  = `CUSIPByISIN`
	sedolMappingBucket = `CUSIPBySEDOL`
)

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

// ReadData reads Security data from source and pushes batches
func ReadData(c chan *fast_lem.Security) {
	data, err := os.Open(source)
	if err != nil {
		log.Fatalln(err)
	}
	defer data.Close()
	r := fast_lem.NewReader(data)
	r.FieldsPerRecord = 17
	var row []string
	row, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}
	for {
		row, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}
		recordCount++
		security := fast_lem.New(row[ColumnCUSIP],
			row[ColumnISIN],
			row[ColumnSEDOL],
			row[ColumnTicker],
			row[ColumnEntityID],
			row[ColumnIssueType],
			row[ColumnCouponRate],
			row[ColumnMaturityDate])
		c <- security
	}
	close(c)
	return
}

// PersistData stores Securities in batches
func PersistData(c chan *fast_lem.Security) {
	storage.Store(c)
	wg.Done()
	return
}

func checkKnownValue() (string, error) {
	var ISIN = `US00037NMH60`
	var EntityID = `06L3Q8-E`
	response, err := storage.Get(ISIN)
	if err != nil {
		return "", err
	}
	if len(response) < 1 {
		return "", errors.New("Response was empty")
	}
	security := response[0]
	if security.LegalEntityID != EntityID {
		return "", errors.New("Unexpected entity ID: " + security.LegalEntityID)
	}
	return `Test OK: ` + ISIN + ` => ` + fmt.Sprintf("%+v", security), nil
}

func main() {
	start := time.Now()
	c := make(chan *fast_lem.Security, 20000)
	var db *bolt.DB
	var err error
	db, err = bolt.Open(dbfile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	storage, err = fast_lem.NewStorage(db)
	if err != nil {
		log.Fatalln(err)
	}
	go ReadData(c)
	wg.Add(1)
	go PersistData(c)
	wg.Wait()
	fmt.Println("ETL completed in", time.Now().Sub(start).Minutes(), "minutes")
	fmt.Println("Loaded", recordCount, "records")
	sanityCheck, err := checkKnownValue()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(sanityCheck)
	return
}
