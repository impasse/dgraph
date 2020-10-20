/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xidmap

import (
	"unsafe"

	"github.com/dgraph-io/dgraph/x"
	"github.com/impasse/ristretto/z"
)

// Trie is an implementation of Ternary Search Tries to store XID to UID map. It uses Arena to
// allocate nodes in the trie. It is not thread-safe.
type Trie struct {
	root int64
	buf  *z.Buffer
}

// NewTrie would return back a Trie backed by the provided Arena. Trie would assume ownership of the
// Arena. Release must be called at the end to release Arena's resources.
func NewTrie() *Trie {
	buf, err := z.NewBufferWith(32<<20, 1<<34, z.UseMmap)
	x.Check(err)
	ro := buf.AllocateOffset(nodeSz)
	return &Trie{
		root: ro,
		buf:  buf,
	}
}
func (t *Trie) getNode(offset int64) *node {
	if offset == 0 {
		return nil
	}
	data := t.buf.Data(offset)
	return (*node)(unsafe.Pointer(&data[0]))
}

// Get would return the UID for the key. If the key is not found, it would return 0.
func (t *Trie) Get(key string) uint64 {
	return t.get(t.root, key)
}

// Put would store the UID for the key.
func (t *Trie) Put(key string, uid uint64) {
	t.put(t.root, key, uid)
}

// Size returns the size of Arena used by this Trie so far.
func (t *Trie) Size() uint32 {
	return uint32(t.buf.Len())
}

// Release would release the resources used by the Arena.
func (t *Trie) Release() {
	t.buf.Release()
}

// node uses 4-byte offsets to save the cost of storing 8-byte pointers. Also, offsets allow us to
// truncate the file bigger and remap it. This struct costs 24 bytes.
type node struct {
	uid   uint64
	r     byte
	left  int64
	mid   int64
	right int64
}

var nodeSz = int64(unsafe.Sizeof(node{}))

func (t *Trie) get(offset int64, key string) uint64 {
	if len(key) == 0 {
		return 0
	}
	for offset != 0 {
		n := t.getNode(offset)
		r := key[0]
		switch {
		case r < n.r:
			offset = n.left
		case r > n.r:
			offset = n.right
		case len(key[1:]) > 0:
			key = key[1:]
			offset = n.mid
		default:
			return n.uid
		}
	}
	return 0
}

func (t *Trie) put(offset int64, key string, uid uint64) int64 {
	n := t.getNode(offset)
	r := key[0]
	if n == nil {
		offset = t.buf.AllocateOffset(nodeSz)
		n = t.getNode(offset)
		n.r = r
	}

	switch {
	case r < n.r:
		n.left = t.put(n.left, key, uid)

	case r > n.r:
		n.right = t.put(n.right, key, uid)

	case len(key[1:]) > 0:
		n.mid = t.put(n.mid, key[1:], uid)

	default:
		n.uid = uid
	}
	return offset
}
