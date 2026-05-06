package squashfs

import (
	"reflect"
	"testing"
)

func TestSplitPathPreservesBackslashInName(t *testing.T) {
	p := `foo/bar\abc`
	expected := []string{"foo", `bar\abc`}

	actual := splitPath(p)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("splitPath(%q) = %#v, expected %#v", p, actual, expected)
	}
}
