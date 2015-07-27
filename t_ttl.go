/*

This code has some portions copied from github.com/siddontang/ledisdb/blob/master/ledis/t_ttl.go
You shal find the

The MIT License (MIT)

Copyright (c) 2014 siddontang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


*/

package goexpirycache

import (
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/util"
	"log"
	"time"
)

const (
	ExpMetaType byte = 102
	ExpTimeType byte = 103
)

var (
	TypeName = map[byte]string{
		ExpTimeType: "exptime",
		ExpMetaType: "expmeta",
	}
	errExpMetaKey = errors.New("invalid expire meta key")
	errExpTimeKey = errors.New("invalid expire time key")
	errIntNumber  = errors.New("invalid integer")
)

func (c *cache) setNextCheckTime(when int64, force bool) {
	c.ttlCheck.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.ttlCheck.Unlock()
}

func expEncodeTimeKey(key []byte, when int64) []byte {
	buf := make([]byte, len(key)+9)

	pos := 0

	buf[pos] = ExpTimeType
	pos++

	binary.BigEndian.PutUint64(buf[pos:], uint64(when))
	pos += 8

	copy(buf[pos:], key)

	return buf
}

func expEncodeMetaKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)

	pos := 0
	buf[pos] = ExpMetaType
	pos++

	copy(buf[pos:], key)

	return buf
}

func expDecodeMetaKey(mk []byte) ([]byte, error) {
	pos := 0
	if mk[pos] != ExpMetaType {
		return nil, errExpMetaKey
	}

	return mk[pos+2:], nil
}

func expDecodeTimeKey(tk []byte) ([]byte, int64, error) {
	pos := 0
	if tk[pos] != ExpTimeType {
		return nil, 0, errExpTimeKey
	}

	return tk[pos+10:], int64(binary.BigEndian.Uint64(tk[pos+1:])), nil
}

func (c *cache) check() {
	now := time.Now().Unix()

	c.ttlCheck.Lock()
	nc := c.nc
	c.ttlCheck.Unlock()

	if now < nc {
		return
	}

	nc = now + 3600

	minKey := expEncodeTimeKey(nil, 0)
	maxKey := expEncodeTimeKey(nil, nc)

	iter := c.db.NewIterator(&util.Range{Start: minKey, Limit: maxKey}, nil)
	for iter.Next() {
		// Use key/value.
		tk := iter.Key()
		mk := iter.Value()

		k, nt, err := expDecodeTimeKey(tk)
		if err != nil {
			continue
		}

		if nt > now {
			//the next ttl check time is nt!
			nc = nt
			break
		}

		c.ttlCheck.Lock()
		if exp, err := Int64(c.db.Get(mk, nil)); err == nil {
			// check expire again
			if exp <= now {
				c.db.Delete(k, nil)
				c.db.Delete(tk, nil)
				c.db.Delete(mk, nil)
			}

		}
		c.ttlCheck.Unlock()
	}
	iter.Release()
	err := iter.Error()

	if err != nil {
		log.Println("Iterator Error: ", err)
	}

	c.setNextCheckTime(nc, false)

	return
}

func Int64(v []byte, err error) (int64, error) {
	if err != nil {

	} else if len(v) != 8 {
		return 0, errIntNumber
	}

	return int64(binary.LittleEndian.Uint64(v)), nil
}
