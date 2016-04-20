package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

var (
	bigdb string
)

func init() {
	flag.StringVar(&bigdb, "bigdb", "../db/lem.db", "path to the database to be dumped")
	flag.Parse()
}
func main() {
	var db *bolt.DB
	var err error
	db, err = bolt.Open(bigdb, 0600, &bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
	if err != nil {
		log.Fatalln("Error opening db:", err)
	}
	defer db.Close()
	if err != nil {
		log.Fatalln(err)
	}
	err = db.View(func(tx *bolt.Tx) error {
		_, err = tx.WriteTo(os.Stdout)
		return err
	})
	if err != nil {
		log.Fatalln(err)
	}
}
