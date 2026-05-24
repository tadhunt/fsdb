package fsdb

import "testing"

func TestValidateDocname(t *testing.T) {
	cases := []struct {
		name    string
		docname string
		ok      bool
	}{
		// Accepted: typical Firestore doc paths.
		{"normal 4-segment", "magic-links/codes/items/123456", true},
		{"normal 2-segment", "profiles/abc-123", true},
		{"under-profile 4-segment", "profiles/abc-123/magic-accounts/uuid-1", true},
		{"single-segment doc", "foo", true},
		{"underscores inside segment", "foo_bar", true},
		{"single leading underscore", "_foo", true},
		{"single trailing underscore", "foo_", true},

		// Rejected: empty / empty segments.
		{"empty", "", false},
		{"trailing slash", "magic-links/codes/items/", false},
		{"leading slash", "/magic-links/codes/items/foo", false},
		{"double slash", "magic-links//items/foo", false},

		// Rejected: Firestore __.*__ reserved segments, anywhere in the path.
		{"reserved doc id", "magic-links/codes/items/__foo__", false},
		{"reserved minimal", "foo/__a__", false},
		{"reserved bookends only", "foo/__", false},
		{"reserved collection", "__system__/codes/items/123", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateDocname(c.docname)
			gotOK := err == nil
			if gotOK != c.ok {
				t.Errorf("validateDocname(%q) = err=%v, want ok=%v", c.docname, err, c.ok)
			}
			if err != nil && !ErrorIsNotFound(err) {
				t.Errorf("validateDocname(%q) error %v is not NotFound — callers' ErrorIsNotFound path won't catch it", c.docname, err)
			}
		})
	}
}
