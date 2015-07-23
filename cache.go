package goexpirycache

import (
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

//Cache This interface exposes the functions which
//
type Cache interface {
	Get(key string)
	Set(key string, value *[]byte, timestamp int64, expiry int64)
	Del(key string, timestamp int64)
	Touch(key string, expiry int64)
}

type cache struct {
	dbName string
	db     *leveldb.DB
}

func New(dbName string) (Cache, error) {
	filePath := "var/" + dbName
	db, err := leveldb.OpenFile(filePath, nil)
	if err != nil {
		log.Println("error opening leveldb file \n")
		return nil, err
	}

	return &cache{dbName: dbName, db: db}, nil
}
