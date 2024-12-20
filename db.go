package fsdb

import (
	"context"

	"cloud.google.com/go/firestore"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/tadhunt/logger"
)

type DBConnection struct {
        log     logger.CompatLogWriter
	project string
	Client  *firestore.Client
}

type DocumentIterator struct {
	*firestore.DocumentIterator
}

type CollectionIterator struct {
	*firestore.CollectionIterator
}

type DbWhere struct {
	Attr       string
	Comparison string
	Val        string
}

var DBIteratorDone = iterator.Done

func NewDBConnection(ctx context.Context, log logger.CompatLogWriter, project string, credentialsFile string) (*DBConnection, error) {
	options := []option.ClientOption{
		option.WithCredentialsFile(credentialsFile),
	}

	client, err := firestore.NewClient(ctx, project, options...)
	if err != nil {
		return nil, err
	}

	dbc := &DBConnection{
		log:     log,
		project: project,
		Client:  client,
	}

	return dbc, nil
}

func (db *DBConnection) Add(ctx context.Context, docname string, dval interface{}) error {
	dref := db.Client.Doc(docname)
	if dref == nil {
		return db.log.ErrFmt("nil dref: bad docname '%s'?", docname)
	}

	wr, err := dref.Create(ctx, dval)
	if err != nil {
		return err
	}

	db.log.Debugf("docname %s dval %#v: wr: %#v", docname, dval, wr)

	return nil
}

func (db *DBConnection) AddOrReplace(ctx context.Context, docname string, dval interface{}) error {
	dref := db.Client.Doc(docname)

	wr, err := dref.Set(ctx, dval)
	if err != nil {
		return err
	}

	db.log.Debugf("docname %s dval %#v: wr: %#v", docname, dval, wr)

	return nil
}

func (db *DBConnection) Delete(ctx context.Context, docname string) error {
	dref := db.Client.Doc(docname)

	_, err := dref.Delete(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (db *DBConnection) Get(ctx context.Context, docname string, dval interface{}) error {
	dref := db.Client.Doc(docname)

	dsnap, err := dref.Get(ctx)
	if err != nil {
		return err
	}

	return dsnap.DataTo(dval)
}

func (db *DBConnection) QueryIterator(ctx context.Context, colname string, attr string, comparison string, val string) *DocumentIterator {
	col := db.Client.Collection(colname)

	query := col.Where(attr, comparison, val)

	iter := query.Documents(ctx)

	return &DocumentIterator{iter}
}

/*
 * Adds a new [automatically named] document to a collection group
 */
func (db *DBConnection) CollectionGroupAdd(ctx context.Context, colname string, dval interface{}) error {
	col := db.Client.Collection(colname)

	dref := col.NewDoc()

	wr, err := dref.Set(ctx, dval)
	if err != nil {
		return err
	}

	db.log.Debugf("colname %s dpath %s dval %#v: wr: %#v", colname, dref.Path, dval, wr)

	return nil
}

func (db *DBConnection) CollectionGroupQuery(ctx context.Context, colname string, wheres []*DbWhere) *DocumentIterator {
	col := db.Client.CollectionGroup(colname)

	var query firestore.Query

	for i, w := range wheres {
		if i == 0 {
			query = col.Where(w.Attr, w.Comparison, w.Val)
		} else {
			query = query.Where(w.Attr, w.Comparison, w.Val)
		}
	}

	iter := query.Documents(ctx)

	return &DocumentIterator{iter}
}

func (db *DBConnection) NextDoc(ctx context.Context, iter *DocumentIterator, dval interface{}) error {
	dsnap, err := iter.Next()
	if err == iterator.Done {
		return DBIteratorDone
	}
	if err != nil {
		return err
	}

	err = dsnap.DataTo(dval)
	if err != nil {
		return err
	}

	return nil
}

func (db *DBConnection) NextDocPath(ctx context.Context, iter *DocumentIterator, dval interface{}) (string, error) {
	dsnap, err := iter.Next()
	if err == iterator.Done {
		return "", DBIteratorDone
	}
	if err != nil {
		return "", err
	}

	err = dsnap.DataTo(dval)
	if err != nil {
		return "", err
	}

	return dsnap.Ref.Path, nil
}

func (db *DBConnection) DocumentIterator(ctx context.Context, path string) *DocumentIterator {
	iter := db.Client.Collection(path).Documents(ctx)
	if iter == nil {
		return nil
	}

	return &DocumentIterator{iter}
}

func (db *DBConnection) CollectionIterator(ctx context.Context, docname string) *CollectionIterator {
	iter := db.Client.Doc(docname).Collections(ctx)
	if iter == nil {
		return nil
	}

	return &CollectionIterator{iter}
}

func (db *DBConnection) Escape(raw string) string {
	s := ""
	for _, c := range raw {
		if c == '|' {
			s += "\\|"
		} else if c == '/' {
			s += "|"
		} else {
			s += string(c)
		}
	}

	return s
}

func (db *DBConnection) Unescape(s string) string {
	r := ""
	escaped := false

	for _, c := range s {
		if c == '\\' {
			if !escaped {
				escaped = true
			} else {
				r += "\\"
			}
			continue
		}

		if escaped {
			if c == '|' {
				r += "|"
			} else {
				r += "\\" + string(c)
			}
		} else if c == '|' {
			r += "/"
		} else {
			r += string(c)
		}

		escaped = false
	}

	return r
}
