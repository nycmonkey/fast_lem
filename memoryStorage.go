package fast_lem

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/smartystreets/mafsa"
)

type SecurityMaster struct {
	Index      *mafsa.MinTree
	Securities []*Security
	ISINIndex  map[string]int
	SEDOLIndex map[string]int
}

var (
	ERR_COUNT_TOO_LOW = errors.New("Provided count was less than actual number of records")
)

// NewSecurityMaster returns an in-memory security master from a channel of securities which
// MUST be sorted in ascending order by CUSIP
func NewSecurityMaster(securities chan *Security) (m *SecurityMaster, err error) {
	m = &SecurityMaster{Securities: make([]*Security, 0), ISINIndex: make(map[string]int), SEDOLIndex: make(map[string]int)}
	i := 0
	bt := mafsa.New()
	for s := range securities {
		err = bt.Insert(s.CUSIP)
		if err != nil {
			return
		}
		if len(s.ISIN) == 12 {
			m.ISINIndex[s.ISIN] = i
		}
		if len(s.SEDOL) == 7 {
			m.SEDOLIndex[s.SEDOL] = i
		}
		m.Securities = append(m.Securities, s)
		i++
	}
	var tmp *os.File
	tmp, err = ioutil.TempFile("", "mafsaDump")
	if err != nil {
		return
	}
	tmp.Close()
	defer os.Remove(tmp.Name())
	err = bt.Save(tmp.Name())
	if err != nil {
		return
	}
	bt = nil
	// debug.FreeOSMemory()
	m.Index, err = mafsa.Load(tmp.Name())
	return
}

// Get "hydrates" security details from one or more identifiers
func (m *SecurityMaster) Get(keys ...string) (response []*Security, err error) {
	size := len(keys)
	response = make([]*Security, size)
	i := 0
	for _, k := range keys {
		var s *Security
		s, err = m.get(k)
		if err != nil {
			return
		}
		response[i] = s
		i++
	}
	return
}

func (m *SecurityMaster) get(key string) (s *Security, err error) {
	s = &Security{}
	switch len(key) {
	case 12:
		idx, ok := m.ISINIndex[key]
		if !ok {
			return
		}
		return m.Securities[idx], nil
	case 7:
		idx, ok := m.SEDOLIndex[key]
		if !ok {
			return
		}
		return m.Securities[idx], nil
	default:
		_, pos := m.Index.IndexedTraverse([]rune(key))
		if pos < 0 {
			return
		}
		return m.Securities[pos-1], nil
	}
	return
}
