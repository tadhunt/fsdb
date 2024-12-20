package fsdb

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"github.com/tadhunt/logger"
)

type DBCollectionChanges struct {
	log  logger.CompatLogWriter
	snap *firestore.QuerySnapshot
}

type DocumentChangeKind int

const (
	DBCHANGE_ERROR DocumentChangeKind = iota
	DBCHANGE_DOC_ADDED
	DBCHANGE_DOC_REMOVED
	DBCHANGE_DOC_CHANGED
)

func (k DocumentChangeKind) ToString() string {
	switch k {
	case DBCHANGE_ERROR:
		return "DBCHANGE_ERROR"
	case DBCHANGE_DOC_ADDED:
		return "DBCHANGE_DOC_ADDED"
	case DBCHANGE_DOC_REMOVED:
		return "DBCHANGE_DOC_REMOVED"
	case DBCHANGE_DOC_CHANGED:
		return "DBCHANGE_DOC_CHANGED"
	}

	return fmt.Sprintf("DBCHANGE_UNKNOWN_%d", k)
}

type DocumentChange struct {
	Kind DocumentChangeKind
	Path string
	doc  *firestore.DocumentSnapshot
}

func (db *DBConnection) DocListen(ctx context.Context, collection string, doc string, handler func(change *DocumentChange) error) error {
	it := db.Client.Collection(collection).Doc(doc).Snapshots(ctx)

	for {
		snap, err := it.Next()

		if err != nil {
			return err
		}

		change := &DocumentChange{
			Path: snap.Ref.Path,
			doc:  snap,
		}

		if !snap.Exists() {
			change.Kind = DBCHANGE_DOC_REMOVED
		} else {
			change.Kind = DBCHANGE_DOC_CHANGED
		}

		err = handler(change)
		if err != nil {
			return err
		}
	}
}

type ListenFilter struct {
	Path  string
	Op    string
	Value interface{}
}

func (db *DBConnection) CollectionListen(log logger.CompatLogWriter, ctx context.Context, collection string, handler func(changes *DBCollectionChanges) error, filter *ListenFilter) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	query := db.Client.Collection(collection).Query
	if filter != nil {
		switch filter.Op {
		case "in":
			fallthrough
		case "not-in":
			a, ok := filter.Value.([]string)
			if !ok {
				return fmt.Errorf("Filter %#v: Filter.Value must be []string", *filter) // TODO(tadhunt): maybe not true, but good enough for my usage for now
			}
			if len(a) > 10 {
				// According to https://firebase.google.com/docs/firestore/query-data/queries#in_not-in_and_array-contains-any
				return fmt.Errorf("Filter %#v: too many values, 10 max", *filter)
			}
		}

		query = query.Where(filter.Path, filter.Op, filter.Value)
	}

	iterator := query.Snapshots(ctx)
	for {
		snap, err := iterator.Next()

		if err != nil {
			return err
		}

		changes := &DBCollectionChanges{
			log: log,
			snap: snap,
		}

		err = handler(changes)
		if err != nil {
			return err
		}
	}
}

func (dc *DocumentChange) Data() map[string]interface{} {
	return dc.doc.Data()
}

func (dc *DocumentChange) DataTo(dval interface{}) error {
	return dc.doc.DataTo(dval)
}

func (c *DBCollectionChanges) Iterator() *DocumentIterator {
	return &DocumentIterator{c.snap.Documents}
}

func (c *DBCollectionChanges) Changes() []*DocumentChange {
	changes := make([]*DocumentChange, 0)

	for _, fc := range c.snap.Changes {
		dc := &DocumentChange{
			Kind: c.kindRemap(fc.Kind),
			Path: fc.Doc.Ref.Path,
			doc:  fc.Doc,
		}
		changes = append(changes, dc)
	}
	return changes
}

func (c *DBCollectionChanges) kindRemap(kind firestore.DocumentChangeKind) DocumentChangeKind {
	switch kind {
	case firestore.DocumentAdded:
		return DBCHANGE_DOC_ADDED
	case firestore.DocumentRemoved:
		return DBCHANGE_DOC_REMOVED
	case firestore.DocumentModified:
		return DBCHANGE_DOC_CHANGED
	}

	c.log.Errorf("mcdb missing remap for firestore.DocumentChangeKind %v", kind)

	return DBCHANGE_ERROR
}
