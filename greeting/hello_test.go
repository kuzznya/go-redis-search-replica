package greeting

import (
	"testing"
)

func TestHello(t *testing.T) {
	hello := Hello("IlyakuzElastiCacheSearch")
	expected := "Hello World - IlyakuzElastiCacheSearch"
	if hello != expected {
		t.Errorf("Expected %s, got %s", expected, hello)
	}
}
