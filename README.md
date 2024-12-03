# cohashing

implement consistent hashing using golang

## options

```golang
// Options represents the options for the hash ring
type Options struct {
	// Number of replicas for each node
	Replicas int

	// Hash function to use
	Hash hash.Hash
}
```

## example

```bash
go get github.com/ISSuh/cohashing
```

use default hash(sha1)

```golang
pakcage (
    "github.com/ISSuh/cohashing"
)

type node struct {
    ID string
    IP string
}

func main() {
	ring := New[node]()

	n1 := node{ID: "node1", IP: "xxx.xxx.xxx.xxx"}
	n2 := node{ID: "node2", IP: "xxx.xxx.xxx.xxx"}

	ring.Put(n1.ID, n1)
	ring.Put(n2.ID, n2)

	node := ring.Locate("object-id")

    ...

    ring.Delete("node1")
}
```

set replicae nums or use user selected hash

```golang
pakcage (
    "github.com/ISSuh/cohashing"
)

type node struct {
    ID string
    IP string
}

func main() {
	hasher := sha256.New()
	option := Options{
		Replicas: 100,
		Hash:     hasher,
	}

	ring := NewWithOptions[node](option)

	n1 := node{ID: "node1", IP: "xxx.xxx.xxx.xxx"}
	n2 := node{ID: "node2", IP: "xxx.xxx.xxx.xxx"}

	ring.Put(n1.ID, n1)
	ring.Put(n2.ID, n2)

	node := ring.Locate("object-id")

    ...

    ring.Delete("node1")
}
```