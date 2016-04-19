package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	"github.com/nycmonkey/fast_lem"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	source string
	dbfile string
	wg     = new(sync.WaitGroup)
)

func init() {
	flag.StringVar(&source, "source", "data.csv", "path to the source data")
	flag.StringVar(&dbfile, "output", "data.db",
		"path to a boltdb database where the data will be stored")
	flag.Parse()
}

const (
	batchSize  = 1000
	bucketName = `Securities`
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
	utf8 := transform.NewReader(data, charmap.Windows1252.NewDecoder())
	cleanCSV := transform.NewReader(utf8, &fast_lem.QuoteEscaper{})
	r := csv.NewReader(cleanCSV)
	r.FieldsPerRecord = 17
	r.LazyQuotes = true
	r.Comma = '|'
	var row []string
	row, err = r.Read()
	fmt.Println(strings.Join(row, " #@#@ "))
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

func PersistData(c chan []*fast_lem.Security, db *bolt.DB) {
	for work := range c {
		err := db.Batch(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			for _, s := range work {
				bytes, err := ffjson.Marshal(s)
				smaller := snappy.Encode(nil, bytes)

				os.Stdout.Write(bytes)
				os.Stdout.Write([]byte("\r\n"))
				if err != nil {
					return err
				}
				if len(s.CUSIP) > 0 {
					err = b.Put([]byte(s.CUSIP), smaller)
					if err != nil {
						return err
					}
				}
				if len(s.ISIN) > 0 {
					err = b.Put([]byte(s.ISIN), smaller)
					if err != nil {
						return err
					}
				}
				if len(s.SEDOL) > 0 {
					err = b.Put([]byte(s.SEDOL), smaller)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			log.Fatalln(err)
		}
	}
	wg.Done()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	c := make(chan []*fast_lem.Security, 20)
	var db *bolt.DB
	var err error
	db, err = bolt.Open(dbfile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go PersistData(c, db)
	}
	go ReadData(c)
	wg.Wait()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		v := b.Get([]byte(`31376XVZ8`))
		if v == nil {
			log.Println("Nothing there!")
		}
		if v != nil {
			var js []byte
			js, err = snappy.Decode(nil, v)
			if err != nil {
				log.Println(err)
			}
			os.Stdout.Write(js)
			os.Stdout.Write([]byte("\r\n"))
		}
		return nil
	})
	return
}
