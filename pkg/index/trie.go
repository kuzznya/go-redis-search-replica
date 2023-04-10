package index

import "sort"

// WalkFunc defines some action to take on the given key and value during
// a Trie Walk. Returning a non-nil error will terminate the Walk.
type WalkFunc func(key string, value []DocTermOccurrence) error

// Trier exposes the Trie structure capabilities.
type Trier interface {
	Get(key string) []DocTermOccurrence
	Put(key string, value []DocTermOccurrence) bool
	Add(key string, value DocTermOccurrence)
	Delete(key string) bool
	Walk(walker WalkFunc) error
	WalkPath(key string, walker WalkFunc) error
}

// RuneTrie is a trie of runes with string keys and interface{} values.
// Note that internal nodes have nil values so a stored nil value will not
// be distinguishable and will not be included in Walks.
type RuneTrie struct {
	value    []DocTermOccurrence
	children map[rune]*RuneTrie
}

// NewRuneTrie allocates and returns a new *RuneTrie.
func NewRuneTrie() *RuneTrie {
	return new(RuneTrie)
}

// Get returns the value stored at the given key. Returns nil for internal
// nodes or for nodes with a value of nil.
func (trie *RuneTrie) Get(key string) []DocTermOccurrence {
	node := trie
	for _, r := range key {
		node = node.children[r]
		if node == nil {
			return nil
		}
	}
	return node.value
}

// Put inserts the value into the trie at the given key, replacing any
// existing items. It returns true if the put adds a new value, false
// if it replaces an existing value.
// Note that internal nodes have nil values so a stored nil value will not
// be distinguishable and will not be included in Walks.
func (trie *RuneTrie) Put(key string, value []DocTermOccurrence) bool {
	node := trie
	for _, r := range key {
		child, _ := node.children[r]
		if child == nil {
			if node.children == nil {
				node.children = map[rune]*RuneTrie{}
			}
			child = new(RuneTrie)
			node.children[r] = child
		}
		node = child
	}
	// does node have an existing value?
	isNewVal := node.value == nil
	node.value = value
	return isNewVal
}

func (trie *RuneTrie) Add(key string, value DocTermOccurrence) {
	node := trie
	for _, r := range key {
		child, _ := node.children[r]
		if child == nil {
			if node.children == nil {
				node.children = map[rune]*RuneTrie{}
			}
			child = new(RuneTrie)
			node.children[r] = child
		}
		node = child
	}
	if node.value == nil {
		node.value = []DocTermOccurrence{value}
	} else {
		idx := sort.Search(len(node.value), func(i int) bool {
			return node.value[i].Doc.Key >= value.Doc.Key
		})
		node.value = append(node.value, DocTermOccurrence{})
		if idx < len(node.value)-1 {
			copy(node.value[idx+1:], node.value[idx:])
		}
		node.value[idx] = value

		//node.value = append(node.value, value)
	}
}

// Delete removes the value associated with the given key. Returns true if a
// node was found for the given key. If the node or any of its ancestors
// becomes childless as a result, it is removed from the trie.
func (trie *RuneTrie) Delete(key string) bool {
	path := make([]nodeRune, len(key)) // record ancestors to check later
	node := trie
	for i, r := range key {
		path[i] = nodeRune{r: r, node: node}
		node = node.children[r]
		if node == nil {
			// node does not exist
			return false
		}
	}
	// delete the node value
	node.value = nil
	// if leaf, remove it from its parent's children map. Repeat for ancestor
	// path.
	if node.isLeaf() {
		// iterate backwards over path
		for i := len(key) - 1; i >= 0; i-- {
			parent := path[i].node
			r := path[i].r
			delete(parent.children, r)
			if !parent.isLeaf() {
				// parent has other children, stop
				break
			}
			parent.children = nil
			if parent.value != nil {
				// parent has a value, stop
				break
			}
		}
	}
	return true // node (internal or not) existed and its value was nil'd
}

// Walk iterates over each key/value stored in the trie and calls the given
// walker function with the key and value. If the walker function returns
// an error, the walk is aborted.
// The traversal is depth first with no guaranteed order.
func (trie *RuneTrie) Walk(walker WalkFunc) error {
	return trie.walk("", walker)
}

// WalkPath iterates over each key/value in the path in trie from the root to
// the node at the given key, calling the given walker function for each
// key/value. If the walker function returns an error, the walk is aborted.
func (trie *RuneTrie) WalkPath(key string, walker WalkFunc) error {
	// Get root value if one exists.
	if trie.value != nil {
		if err := walker("", trie.value); err != nil {
			return err
		}
	}

	for i, r := range key {
		if trie = trie.children[r]; trie == nil {
			return nil
		}
		if trie.value != nil {
			if err := walker(key[0:i+1], trie.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// RuneTrie node and the rune key of the child the path descends into.
type nodeRune struct {
	node *RuneTrie
	r    rune
}

func (trie *RuneTrie) walk(key string, walker WalkFunc) error {
	if trie.value != nil {
		if err := walker(key, trie.value); err != nil {
			return err
		}
	}
	for r, child := range trie.children {
		if err := child.walk(key+string(r), walker); err != nil {
			return err
		}
	}
	return nil
}

func (trie *RuneTrie) isLeaf() bool {
	return len(trie.children) == 0
}
