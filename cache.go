package goexpirycache

import(
	"log",
    "github.com/syndtr/goleveldb"
)

type Cache interface{
	Get(key string)
    Set(key string, value *[]byte, timestamp int64, expiry int64)
    Del(key string, timestamp int64) 
    Touch(key string, expiry int64)
}