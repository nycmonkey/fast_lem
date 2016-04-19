package fast_lem

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/pquerna/ffjson/ffjson"
)

type Getter interface {
	Get(keys ...string) (*Response, error)
}

type Storer interface {
	Store(...*Security) error
}

type Storage interface {
	Getter
	Storer
	QueryHandler(w http.ResponseWriter, r *http.Request)
}

type boltPersistance struct {
	db *bolt.DB
}

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

func (s *boltPersistance) Store(securities ...*Security) (err error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = s.db.Update(func(tx *bolt.Tx) error {
		detailsBucket := tx.Bucket([]byte(DetailsBucket))
		isinBucket := tx.Bucket([]byte(IsinBucket))
		sedolBucket := tx.Bucket([]byte(SedolBucket))
		for _, sec := range securities {
			buf.Reset()
			enc.Encode(sec)
			// store the security details in the CUSIP bucket
			if len(sec.CUSIP) > 0 {
				err = detailsBucket.Put([]byte(sec.CUSIP), buf.Bytes())
				if err != nil {
					return err
				}
			}
			// store the CUSIP/CINS corresponding to the ISIN in the ISIN bucket
			if len(sec.ISIN) > 0 {
				err = isinBucket.Put([]byte(sec.ISIN), []byte(sec.CUSIP))
				if err != nil {
					return err
				}
			}
			// store the CUSIP/CINS corresponding to the SEDOL in the SEDOL bucket
			if len(sec.SEDOL) > 0 {
				err = sedolBucket.Put([]byte(sec.SEDOL), []byte(sec.CUSIP))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
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

func (bp *boltPersistance) Get(keys ...string) (response *Response, err error) {
	var storedVal []byte
	buf := new(bytes.Buffer)
	decoder := gob.NewDecoder(buf)
	response = NewResponse()
	sort.Strings(keys)
	err = bp.db.View(func(tx *bolt.Tx) error {
		detailsBucket := tx.Bucket([]byte(DetailsBucket))
		isinBucket := tx.Bucket([]byte(IsinBucket))
		sedolBucket := tx.Bucket([]byte(SedolBucket))
		var cusip []byte
		for _, k := range keys {
			switch len(k) {
			case 12:
				cusip = isinBucket.Get([]byte(k))
			case 7:
				cusip = sedolBucket.Get([]byte(k))
			default:
				cusip = []byte(k)
			}
			storedVal = detailsBucket.Get(cusip)
			if len(storedVal) == 0 {
				response.Results[k] = &Security{}
				continue
			}
			sec := Security{}
			buf.Reset()
			buf.Write(storedVal)
			err = decoder.Decode(&sec)
			if err != nil {
				return err
			}
			response.Results[k] = &sec
		}
		return nil
	})
	return
}
