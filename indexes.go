package fsdb

import (
	"encoding/json"
	"io"
	"os"
)

// QueryScope defines the scope of a Firestore index.
type QueryScope string

const (
	// ScopeCollection scopes the index to a single collection.
	ScopeCollection QueryScope = "COLLECTION"
	// ScopeCollectionGroup scopes the index to a collection group.
	ScopeCollectionGroup QueryScope = "COLLECTION_GROUP"
)

// IndexField defines a single field within a composite index.
type IndexField struct {
	FieldPath   string `json:"fieldPath"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

// Index defines a Firestore composite index.
type Index struct {
	CollectionGroup string       `json:"collectionGroup"`
	QueryScope      QueryScope   `json:"queryScope"`
	Fields          []IndexField `json:"fields"`
}

// FieldOverrideIndex is one index of a single-field index control. Unlike a
// composite index, the field is named once by the enclosing FieldOverride, so
// an entry says only how that field is indexed, and at what scope.
//
// QueryScope carries more weight here than it does for a composite index:
// Firestore indexes every field at collection scope on its own, so the usual
// reason to declare an override is to add COLLECTION_GROUP scope, which a
// collection group query requires and which is never created automatically.
type FieldOverrideIndex struct {
	Order       string     `json:"order,omitempty"`
	ArrayConfig string     `json:"arrayConfig,omitempty"`
	QueryScope  QueryScope `json:"queryScope,omitempty"`
}

// FieldOverride defines a single-field index override.
type FieldOverride struct {
	CollectionGroup string               `json:"collectionGroup"`
	FieldPath       string               `json:"fieldPath"`
	Indexes         []FieldOverrideIndex `json:"indexes"`
}

// NewFieldOverride creates a single-field index control for the named field.
func NewFieldOverride(collectionGroup string, fieldPath string) *FieldOverride {
	return &FieldOverride{
		CollectionGroup: collectionGroup,
		FieldPath:       fieldPath,
	}
}

// Asc adds an ascending index at the given scope.
func (f *FieldOverride) Asc(scope QueryScope) *FieldOverride {
	f.Indexes = append(f.Indexes, FieldOverrideIndex{
		Order:      "ASCENDING",
		QueryScope: scope,
	})
	return f
}

// Desc adds a descending index at the given scope.
func (f *FieldOverride) Desc(scope QueryScope) *FieldOverride {
	f.Indexes = append(f.Indexes, FieldOverrideIndex{
		Order:      "DESCENDING",
		QueryScope: scope,
	})
	return f
}

// ArrayContains adds an array-contains index at the given scope.
func (f *FieldOverride) ArrayContains(scope QueryScope) *FieldOverride {
	f.Indexes = append(f.Indexes, FieldOverrideIndex{
		ArrayConfig: "CONTAINS",
		QueryScope:  scope,
	})
	return f
}

// IndexSet is a collection of indexes and field overrides that serializes
// to the firestore.indexes.json format expected by Firebase CLI.
type IndexSet struct {
	Indexes        []*Index         `json:"indexes"`
	FieldOverrides []*FieldOverride `json:"fieldOverrides"`
}

// NewIndex creates a new index builder for the given collection.
// The default query scope is COLLECTION.
func NewIndex(collectionGroup string) *Index {
	return &Index{
		CollectionGroup: collectionGroup,
		QueryScope:      ScopeCollection,
	}
}

// Scope sets the query scope for this index.
func (idx *Index) Scope(scope QueryScope) *Index {
	idx.QueryScope = scope
	return idx
}

// Asc adds an ascending field to this index.
func (idx *Index) Asc(fieldPath string) *Index {
	idx.Fields = append(idx.Fields, IndexField{
		FieldPath: fieldPath,
		Order:     "ASCENDING",
	})
	return idx
}

// Desc adds a descending field to this index.
func (idx *Index) Desc(fieldPath string) *Index {
	idx.Fields = append(idx.Fields, IndexField{
		FieldPath: fieldPath,
		Order:     "DESCENDING",
	})
	return idx
}

// ArrayContains adds an array-contains field to this index.
func (idx *Index) ArrayContains(fieldPath string) *Index {
	idx.Fields = append(idx.Fields, IndexField{
		FieldPath:   fieldPath,
		ArrayConfig: "CONTAINS",
	})
	return idx
}

// NewIndexSet creates an empty IndexSet.
func NewIndexSet() *IndexSet {
	return &IndexSet{
		Indexes:        []*Index{},
		FieldOverrides: []*FieldOverride{},
	}
}

// Add appends one or more index definitions to the set.
func (s *IndexSet) Add(indexes ...*Index) {
	s.Indexes = append(s.Indexes, indexes...)
}

// Remove removes index definitions that are equal to any of the provided indexes.
func (s *IndexSet) Remove(indexes ...*Index) {
	filtered := make([]*Index, 0, len(s.Indexes))
	for _, existing := range s.Indexes {
		if !indexMatchesAny(existing, indexes) {
			filtered = append(filtered, existing)
		}
	}
	s.Indexes = filtered
}

// AddFieldOverride appends one or more field overrides to the set.
func (s *IndexSet) AddFieldOverride(overrides ...*FieldOverride) {
	s.FieldOverrides = append(s.FieldOverrides, overrides...)
}

// WriteJSON writes the index set as JSON to the given writer.
func (s *IndexSet) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}

// ReadJSON decodes an IndexSet from the given reader.
func ReadJSON(r io.Reader) (*IndexSet, error) {
	s := NewIndexSet()
	if err := json.NewDecoder(r).Decode(s); err != nil {
		return nil, err
	}
	return s, nil
}

// ReadFile loads an IndexSet from the named file.
func ReadFile(path string) (*IndexSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadJSON(f)
}

// WriteFile writes the index set as JSON to the named file.
func (s *IndexSet) WriteFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return s.WriteJSON(f)
}

func indexMatchesAny(idx *Index, targets []*Index) bool {
	for _, t := range targets {
		if indexEqual(idx, t) {
			return true
		}
	}
	return false
}

func indexEqual(a, b *Index) bool {
	if a.CollectionGroup != b.CollectionGroup || a.QueryScope != b.QueryScope {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}
