package goexpirycache

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"sync"
	"time"
)

const (
	TTLCheckInterval = 15 * 60 //seconds; 15 minutes
)

var (
	errKeyNotFound = errors.New("key not found")
)

//Cache This interface exposes the functions which
//
type Cache interface {
	Get(key []byte) (*[]byte, int64, error)
	Set(key []byte, value *[]byte, timestamp int64, expiry int64)
	Del(key []byte, timestamp int64)
}

type cache struct {
	dbName string
	db     *leveldb.DB

	ttlCheck sync.Mutex
	nc       int64 //next check time
}

func New(dbName string) (Cache, error) {

	filePath := "var/" + dbName

	db, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		log.Println("error opening leveldb file \n")
		return nil, err
	}

	now := time.Now()
	tomorrow := now.AddDate(0, 0, 1)

	c := &cache{dbName: dbName, db: db, nc: tomorrow.Unix()}
	go c.expiryCheck()

	return c, nil
}

func (c *cache) expiryCheck() {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered in expiry check", r)
			go c.expiryCheck()
		}
	}()

	tick := time.NewTicker(time.Duration(TTLCheckInterval) * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.check()
		}
	}
}

func (c *cache) Get(key []byte) (value *[]byte, timestamp int64, err error) {
	if b := c.isStale(key); b {
		err = errKeyNotFound
		return
	}
	tkey := append(key, []byte("timestamp")...)
	timestamp, err = Int64(c.db.Get(tkey, nil))
	if err != nil {
		return
	}
	val, err := c.db.Get(key, nil)
	value = &val
	return
}

func (c *cache) Del(key []byte, timestamp int64) {
	tkey := append(key, []byte("timestamp")...)
	keyTimestamp, err := Int64(c.db.Get(tkey, nil))
	if err != nil {
		return
	}

	if timestamp < keyTimestamp {
		return
	}
	mk := expEncodeMetaKey(key)
	expiry, err1 := Int64(c.db.Get(mk, nil))
	tk := []byte{}
	if err1 == nil {
		tk = expEncodeTimeKey(key, expiry)
	}

	batch := new(leveldb.Batch)
	batch.Delete(key)
	batch.Delete(tk)
	batch.Delete(mk)
	err = c.db.Write(batch, nil)
}

func (c *cache) Set(key []byte, value *[]byte, timestamp int64, expiry int64) {
	tkey := append(key, []byte("timestamp")...)
	keyTimestamp, err := Int64(c.db.Get(tkey, nil))
	if err != nil {
		return
	}

}

func (c *cache) isStale(key []byte) bool {
	now := time.Now().Unix()
	mk := expEncodeMetaKey(key)
	exp, err := Int64(c.db.Get(mk, nil))
	if err != nil {
		return true
	}
	return exp <= now
}
