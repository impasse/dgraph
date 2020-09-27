package xidmap

import (
	"io/ioutil"
	"testing"
)

func TestRocksDB(t *testing.T) {
	path, _ := ioutil.TempDir("", "rocksdb-test")
	r := NewRocksDB(path)
	r.Set("aaaaaa", 64)
	if r.Get("aaaaaa") != 64 {
		t.Fail()
	}
}
