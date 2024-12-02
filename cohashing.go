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
type Ring struct {
	keys  []uint64
	nodes map[uint64]Node
	items map[string]struct{}

	replicas int
	hasher   hash.Hash

	m sync.RWMutex
}

// Node represents a node in the hash ring
type Node struct {
	ID   string
	Hash uint64
}

// Options represents the options for the hash ring
type Options struct {
	Replicas int
	Hash     hash.Hash64
}

// Creates a new Ring with options
func NewWithOptions(options Options) *Ring {
	return &Ring{
		keys:     make([]uint64, 0),
		nodes:    make(map[uint64]Node),
		items:    make(map[string]struct{}),
		replicas: options.Replicas,
		hasher:   options.Hash,
	}
}

// Creates a new Ring with default options
func New() *Ring {
	return &Ring{
		keys:     make([]uint64, 0),
		nodes:    make(map[uint64]Node),
		items:    make(map[string]struct{}),
		replicas: defaultReplicas,
		hasher:   sha1.New(),
	}
}

// Put a node in the hash ring
func (r *Ring) Put(id string) {
	r.m.Lock()
	defer r.m.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", id, i)
		hash := r.hashKey(key)
		node := Node{
			ID:   id,
			Hash: hash,
		}

		r.nodes[hash] = node
		r.keys = append(r.keys, hash)
	}

	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})

	r.items[id] = struct{}{}
}

// Delete a node from the hash ring
func (r *Ring) Delete(id string) {
	r.m.Lock()
	defer r.m.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s:%d", id, i)
		hash := r.hashKey(key)

		delete(r.nodes, hash)
		r.deleteKey(hash)
	}

	delete(r.items, id)
}

// Get returns the node for a given key
func (r *Ring) Get(id string) string {
	r.m.RLock()
	defer r.m.RUnlock()

	hash := r.hashKey(id)
	for _, k := range r.keys {
		if hash <= k {
			return r.nodes[k].ID
		}
	}

	return r.nodes[r.keys[0]].ID // Wrap around to the first node
}

// AllItems returns all the items in the ring
func (r *Ring) AllItems() []string {
	r.m.RLock()
	defer r.m.RUnlock()

	items := make([]string, 0)
	for item := range r.items {
		items = append(items, item)
	}
	return items
}

// Len returns the number of items in the ring
func (r *Ring) Len() int {
	r.m.RLock()
	defer r.m.RUnlock()
	return len(r.items)
}

// Delete Key from the sorted keys
func (r *Ring) deleteKey(key uint64) {
	for i, k := range r.keys {
		if k == key {
			r.keys = append(r.keys[:i], r.keys[i+1:]...)
		}
	}
}

// hashKey generates a hash for a given key
func (r *Ring) hashKey(key string) uint64 {
	r.hasher.Reset()
	r.hasher.Write([]byte(key))
	b := r.hasher.Sum(nil)
	return binary.BigEndian.Uint64(b[:8])
}
