package cohashing

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"
	"sync"
)

const (
	defaultReplicas = 20
)

// Ring represents the consistent hash ring
type Ring[T any] struct {
	keys  []uint64
	nodes map[uint64]node[T]
	items map[string]T

	replicas int
	hasher   hash.Hash

	m sync.RWMutex
}

// Node represents a node in the hash ring
type node[T any] struct {
	Item T
	Hash uint64
}

// Options represents the options for the hash ring
type Options struct {
	// Number of replicas for each node
	Replicas int

	// Hash function to use
	Hash hash.Hash
}

// Creates a new Ring with options
func NewWithOptions[T any](options Options) *Ring[T] {
	return &Ring[T]{
		keys:     make([]uint64, 0),
		nodes:    make(map[uint64]node[T]),
		items:    make(map[string]T),
		replicas: options.Replicas,
		hasher:   options.Hash,
	}
}

// Creates a new Ring with default options
func New[T any]() *Ring[T] {
	return &Ring[T]{
		keys:     make([]uint64, 0),
		nodes:    make(map[uint64]node[T]),
		items:    make(map[string]T),
		replicas: defaultReplicas,
		hasher:   sha1.New(),
	}
}

// Put a node in the hash ring
func (r *Ring[T]) Put(id string, item T) {
	r.m.Lock()
	defer r.m.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", id, i)
		hash := r.hashKey(key)
		r.addNode(hash, item)
	}

	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})

	r.items[id] = item
}

// Delete a node from the hash ring
func (r *Ring[T]) Delete(id string) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.items[id]; !ok {
		return
	}

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", id, i)
		hash := r.hashKey(key)
		r.deleteNode(hash)
	}

	delete(r.items, id)
}

// Get returns the nearby node for a given key
func (r *Ring[T]) Locate(id string) T {
	r.m.RLock()
	defer r.m.RUnlock()

	hash := r.hashKey(id)
	index := r.search(hash)
	key := r.keys[index]
	return r.nodes[key].Item
}

// AllItems returns all the items in the ring
func (r *Ring[T]) AllItems() []T {
	r.m.RLock()
	defer r.m.RUnlock()

	items := make([]T, 0)
	for _, item := range r.items {
		items = append(items, item)
	}
	return items
}

// Len returns the number of items in the ring
func (r *Ring[T]) Len() int {
	r.m.RLock()
	defer r.m.RUnlock()
	return len(r.items)
}

// search returns the index of the key in the sorted keys
func (r *Ring[T]) search(hash uint64) int {
	comp := func(i int) bool {
		return r.keys[i] > hash
	}

	index := sort.Search(len(r.keys), comp)
	if index >= len(r.keys) {
		index = 0
	}
	return index
}

// addNode adds a node to the ring
func (r *Ring[T]) addNode(hash uint64, item T) {
	node := node[T]{
		Item: item,
		Hash: hash,
	}

	r.nodes[hash] = node
	r.keys = append(r.keys, hash)
}

// deleteNode deletes a node from the ring
func (r *Ring[T]) deleteNode(hash uint64) {
	delete(r.nodes, hash)
	r.deleteKey(hash)
}

// Delete Key from the sorted keys
func (r *Ring[T]) deleteKey(key uint64) {
	for i, k := range r.keys {
		if k == key {
			r.keys = append(r.keys[:i], r.keys[i+1:]...)
		}
	}
}

// hashKey generates a hash for a given key
func (r *Ring[T]) hashKey(key string) uint64 {
	r.hasher.Reset()
	r.hasher.Write([]byte(key))
	b := r.hasher.Sum(nil)
	return binary.BigEndian.Uint64(b[:8])
}
