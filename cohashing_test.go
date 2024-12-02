package cohashing

import (
	"testing"
)

func Test_Ring_Put(t *testing.T) {
	ring := New()

	ring.Put("node1")
	ring.Put("node2")

	if len(ring.keys) != 2*defaultReplicas {
		t.Errorf("expected %d keys, got %d", 2*defaultReplicas, len(ring.keys))
	}

	if len(ring.nodes) != 2*defaultReplicas {
		t.Errorf("expected %d nodes, got %d", 2*defaultReplicas, len(ring.nodes))
	}
}

func Test_Ring_Delete(t *testing.T) {
	ring := New()

	ring.Put("node1")
	ring.Put("node2")
	ring.Delete("node1")

	if len(ring.keys) != defaultReplicas {
		t.Errorf("expected %d keys, got %d", defaultReplicas, len(ring.keys))
	}

	if len(ring.nodes) != defaultReplicas {
		t.Errorf("expected %d nodes, got %d", defaultReplicas, len(ring.nodes))
	}
}

func Test_Ring_Get(t *testing.T) {
	ring := New()

	ring.Put("node1")
	ring.Put("node2")
	ring.Put("node3")

	node := ring.Get("somekey")

	if node != "node1" && node != "node2" && node != "node3" {
		t.Errorf("expected node1, node2, or node3, got %s", node)
	}

	// Test wrap around
	node = ring.Get("anotherkey")
	if node != "node1" && node != "node2" && node != "node3" {
		t.Errorf("expected node1, node2, or node3, got %s", node)
	}
}

func Test_Ring_AllItems(t *testing.T) {
	ring := New()

	ring.Put("node1")
	ring.Put("node2")

	items := ring.AllItems()
	if len(items) != 2 {
		t.Errorf("expected %d nodes, got %d", 2, len(items))
	}
}

func Test_Ring_Len(t *testing.T) {
	ring := New()

	ring.Put("node1")
	ring.Put("node2")

	if ring.Len() != 2 {
		t.Errorf("expected %d nodes, got %d", 2, ring.Len())
	}
}
