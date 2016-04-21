package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/boltdb/bolt"
	"github.com/nycmonkey/fast_lem"
)

var (
	source  string
	dbfile  string
	port    int
	storage fast_lem.Getter
)

func init() {
	flag.IntVar(&port, "port", 8888, "port on which the server will listen")
	flag.StringVar(&dbfile, "dbfile", "../db/lem.db",
		"path to a boltdb database where the data will be stored")
	flag.Parse()
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
	var db *bolt.DB
	var err error
	db, err = bolt.Open(dbfile, 0666, &bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
	if err != nil {
		log.Fatalln("Error opening db:", err)
	}
	defer db.Close()
	storage = fast_lem.NewGetter(db)
	sanityCheck, err := checkKnownValue()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(sanityCheck)
	http.HandleFunc("/query", storage.QueryHandler)
	listen := fmt.Sprintf(":%d", port)
	fmt.Println("Listening on", listen)
	log.Fatal(http.ListenAndServe(listen, nil))
	return
}
