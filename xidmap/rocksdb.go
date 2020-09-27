package xidmap

import (
	"encoding/binary"
	"github.com/dgraph-io/dgraph/x"
	"github.com/linxGnu/grocksdb"
)

type RocksDB struct {
	db          *grocksdb.DB
	readOption  *grocksdb.ReadOptions
	writeOption *grocksdb.WriteOptions
}

func NewRocksDB(name string) *RocksDB {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, name)
	x.Check(err)
	return &RocksDB{
		db,
		ro,
		wo,
	}
}

func (r *RocksDB) Get(key string) uint64 {
	val, err := r.db.Get(r.readOption, []byte(key))
	x.Check(err)
	if val.Exists() {
		return binary.BigEndian.Uint64(val.Data())
	} else {
		return 0
	}
}

func (r *RocksDB) Set(key string, val uint64) {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, val)
	err := r.db.Put(r.writeOption, []byte(key), v)
	x.Check(err)
}
