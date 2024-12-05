/*
MIT License

Copyright (c) 2024 ISSuh

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

package cohashing

import (
	"crypto/sha256"
	"testing"
)

type item struct {
	ID    string
	Value string
}

func Test_Ring_Put(t *testing.T) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)

	if len(ring.keys) != 2*defaultReplicas {
		t.Errorf("expected %d keys, got %d", 2*defaultReplicas, len(ring.keys))
	}

	if len(ring.nodes) != 2*defaultReplicas {
		t.Errorf("expected %d nodes, got %d", 2*defaultReplicas, len(ring.nodes))
	}
}

func Test_Ring_Delete(t *testing.T) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)

	ring.Delete(n1ID)

	if len(ring.keys) != defaultReplicas {
		t.Errorf("expected %d keys, got %d", defaultReplicas, len(ring.keys))
	}

	if len(ring.nodes) != defaultReplicas {
		t.Errorf("expected %d nodes, got %d", defaultReplicas, len(ring.nodes))
	}
}

func Test_Ring_Get(t *testing.T) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n3ID := "node3"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}
	n3 := item{ID: n3ID, Value: "value3"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)
	ring.Put(n3ID, n3)

	node := ring.Locate("12345")
	if node.ID != "node1" && node.ID != "node2" && node.ID != "node3" {
		t.Errorf("expected node1, node2, or node3, got %s", node)
	}

	node = ring.Locate("asdasdfsda")
	if node.ID != "node1" && node.ID != "node2" && node.ID != "node3" {
		t.Errorf("expected node1, node2, or node3, got %s", node)
	}
}

func Test_Ring_Len(t *testing.T) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n3ID := "node3"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}
	n3 := item{ID: n3ID, Value: "value3"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)
	ring.Put(n3ID, n3)

	if ring.Len() != 3 {
		t.Errorf("expected %d nodes, got %d", 2, ring.Len())
	}
}

func Test_Ring_AllItems(t *testing.T) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n3ID := "node3"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}
	n3 := item{ID: n3ID, Value: "value3"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)
	ring.Put(n3ID, n3)

	items := ring.AllItems()
	if len(items) != ring.Len() {
		t.Errorf("expected %d nodes, got %d", 2, len(items))
	}
}

func Test_Ring_use_other_hash(t *testing.T) {
	hasher := sha256.New()
	option := Options{
		Replicas: 100,
		Hash:     hasher,
	}

	ring := NewWithOptions[item](option)

	n1ID := "node1"
	n2ID := "node2"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)

	if len(ring.keys) != 2*option.Replicas {
		t.Errorf("expected %d keys, got %d", 2*option.Replicas, len(ring.keys))
	}

	if len(ring.nodes) != 2*option.Replicas {
		t.Errorf("expected %d nodes, got %d", 2*option.Replicas, len(ring.nodes))
	}

	node := ring.Locate("12345")
	if node.ID != "node1" && node.ID != "node2" {
		t.Errorf("expected node1 or node2, got %s", node)
	}

	ring.Delete(n1ID)

	if len(ring.keys) != option.Replicas {
		t.Errorf("expected %d keys, got %d", option.Replicas, len(ring.keys))
	}

	if len(ring.nodes) != option.Replicas {
		t.Errorf("expected %d nodes, got %d", option.Replicas, len(ring.nodes))
	}
}

func Benchmark_Ring_Put(b *testing.B) {
	ring := New[item]()

	n1ID := "node1"
	n1 := item{ID: n1ID, Value: "value1"}

	for i := 0; i < b.N; i++ {
		ring.Put(n1ID, n1)
	}
}

func Benchmark_Ring_Delete(b *testing.B) {
	ring := New[item]()

	n1ID := "node1"
	n1 := item{ID: n1ID, Value: "value1"}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ring.Put(n1ID, n1)
		b.StartTimer()

		ring.Delete(n1ID)
	}
}

func Benchmark_Ring_Locate(b *testing.B) {
	ring := New[item]()

	n1ID := "node1"
	n2ID := "node2"
	n3ID := "node3"
	n1 := item{ID: n1ID, Value: "value1"}
	n2 := item{ID: n2ID, Value: "value2"}
	n3 := item{ID: n3ID, Value: "value3"}

	ring.Put(n1ID, n1)
	ring.Put(n2ID, n2)
	ring.Put(n3ID, n3)

	for i := 0; i < b.N; i++ {
		ring.Locate("12345")
	}
}
