package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/nycmonkey/fast_lem"
)

var (
	source    string
	dbfile    string
	batchSize int
	port      int
	wg        = new(sync.WaitGroup)
	storage   fast_lem.Storage
)

func init() {
	flag.IntVar(&batchSize, "batch", 1000, "batch size for storing records")
	flag.IntVar(&port, "port", 8888, "batch size for storing records")
	flag.StringVar(&source, "source", "data.csv", "path to the source data")
	flag.StringVar(&dbfile, "output", "data.db",
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

func ReadData(c chan []*fast_lem.Security) {
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
	batch := make([]*fast_lem.Security, batchSize, batchSize)
	i := 0
	for {
		row, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}
		security := fast_lem.New(row[ColumnCUSIP],
			row[ColumnISIN],
			row[ColumnSEDOL],
			row[ColumnTicker],
			row[ColumnEntityID],
			row[ColumnIssueType],
			row[ColumnCouponRate],
			row[ColumnMaturityDate])
		batch[i] = security
		if i == batchSize-1 {
			c <- batch
			i = 0
		}
		i++
	}
	if i > 0 {
		c <- batch[0:i]
	}
	close(c)
}

func PersistData(c chan []*fast_lem.Security) {
	var err error
	for securities := range c {
		err = storage.Store(securities...)
		if err != nil {
			log.Fatalln(err)
		}
	}
	wg.Done()
	return
}

func checkKnownValue() (string, error) {
	var ISIN = `US003554K355`
	var EntityID = `071S60-E`
	response, err := storage.Get(ISIN)
	log.Printf("response: %+v\n", response)
	if err != nil {
		return "", err
	}
	security := response.Results[ISIN]
	log.Printf("security: %+v\n", security)
	if security.LegalEntityID != EntityID {
		return "", errors.New("Unexpected entity ID: " + security.LegalEntityID)
	}
	return `Test OK: ` + ISIN + ` => ` + fmt.Sprintf("%+v", security), nil
}

func main() {
	c := make(chan []*fast_lem.Security, 2)
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
	wg.Add(1)
	go PersistData(c)
	go ReadData(c)
	wg.Wait()
	sanity_check, err := checkKnownValue()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(sanity_check)
	http.HandleFunc("/query", storage.QueryHandler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	return
}
