package fast_lem

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	"github.com/pquerna/ffjson/ffjson"
)

// Getter looks up details of Securities by ID
type Getter interface {
	Get(keys ...string) (*Response, error)
}

// Storer persits Security details
type Storer interface {
	Store(chan *Security)
}

// Storage can store and retrieve Security details, and respond to queries via HTTP
type Storage interface {
	Getter
	Storer
	QueryHandler(w http.ResponseWriter, r *http.Request)
}

type boltPersistance struct {
	db *bolt.DB
}

// NewStorage returns a Security database ready to use
func NewStorage(db *bolt.DB) (Storage, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(DetailsBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(IsinBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(SedolBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return &boltPersistance{db: db}, err
}

func decodeSecurity(encoded []byte) (s *Security, err error) {
	var decompressed []byte
	decompressed, err = snappy.Decode(nil, encoded)
	buf := bytes.NewBuffer(decompressed)
	dec := gob.NewDecoder(buf)
	s = &Security{}
	err = dec.Decode(s)
	return
}

func encodeSecurity(s *Security) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(s)
	return snappy.Encode(nil, buf.Bytes())
}

// Store persists a batch of Securities
func (bp *boltPersistance) Store(securities chan *Security) {
	var err error
	for sec := range securities {
		data := encodeSecurity(sec)
		err = bp.persist([]byte(DetailsBucket), []byte(sec.CUSIP), data)
		if err != nil {
			panic(err)
		}
		if len(sec.ISIN) > 0 {
			err = bp.persist([]byte(IsinBucket), []byte(sec.ISIN), []byte(sec.CUSIP))
			if err != nil {
				panic(err)
			}
		}
		if len(sec.SEDOL) > 0 {
			err = bp.persist([]byte(SedolBucket), []byte(sec.SEDOL), []byte(sec.CUSIP))
			if err != nil {
				panic(err)
			}
		}
	}
	return
}

func (bp *boltPersistance) persist(b, k, v []byte) error {
	return bp.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b)
		return bucket.Put(k, v)
	})
}

// Get "hydrates" security details from one or more identifiers
func (bp *boltPersistance) Get(keys ...string) (response *Response, err error) {
	response = NewResponse()
	for _, k := range keys {
		var s *Security
		s, err = bp.get(k)
		if err != nil {
			return
		}
		response.Results[k] = s
	}
	return
}

func (bp *boltPersistance) get(key string) (s *Security, err error) {
	err = bp.db.View(func(tx *bolt.Tx) error {
		detailsBucket := tx.Bucket([]byte(DetailsBucket))
		var cusip []byte
		switch len(key) {
		case 12:
			isinBucket := tx.Bucket([]byte(IsinBucket))
			cusip = isinBucket.Get([]byte(key))
		case 7:
			sedolBucket := tx.Bucket([]byte(SedolBucket))
			cusip = sedolBucket.Get([]byte(key))
		default:
			cusip = []byte(key)
		}
		if cusip == nil {
			s = &Security{}
			return nil
		}
		encoded := detailsBucket.Get(cusip)
		if encoded == nil {
			s = &Security{}
			return nil
		}
		s, err = decodeSecurity(encoded)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (bp *boltPersistance) QueryHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if len(data) == 0 {
		http.Error(w, "This method expects a JSON body containing a request.  This request had a body of length 0.", http.StatusBadRequest)
		return
	}
	var req = &Request{}
	err = ffjson.Unmarshal(data, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var response *Response
	response, err = bp.Get(req.Keys...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var js []byte
	js, err = ffjson.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
	return
}


