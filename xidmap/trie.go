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
	"github.com/plar/go-adaptive-radix-tree"
)

// Trie is an implementation of Ternary Search Tries to store XID to UID map. It uses Arena to
// allocate nodes in the trie. It is not thread-safe.
type Trie struct {
	tree art.Tree
}

// NewTrie would return back a Trie backed by the provided Arena. Trie would assume ownership of the
// Arena. Release must be called at the end to release Arena's resources.
func NewTrie() *Trie {
	return &Trie{
		tree: art.New(),
	}
}

// Get would return the UID for the key. If the key is not found, it would return 0.
func (t *Trie) Get(key string) uint64 {
	v, f := t.tree.Search(art.Key(key))
	if f {
		return v.(uint64)
	} else {
		return 0
	}
}

// Put would store the UID for the key.
func (t *Trie) Put(key string, uid uint64) {
	t.tree.Insert(art.Key(key), uid)
}

// Release would release the resources used by the Arena.
func (t *Trie) Release() {
	t.tree = nil
}
