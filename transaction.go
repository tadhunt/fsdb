package fsdb

import (
	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"context"
	"fmt"
)

type TransactionFunc func(ctx context.Context, t *Transaction) error

type Transaction struct {
	db     *DBConnection
	ft     *firestore.Transaction
	tfuncs []TransactionFunc
}

func (db *DBConnection) RunTransaction(ctx context.Context, tfuncs ...TransactionFunc) error {
	transaction := &Transaction{
		db:     db,
		tfuncs: tfuncs,
	}

	return db.Client.RunTransaction(ctx, transaction.handler)
}

func (t *Transaction) handler(ctx context.Context, ft *firestore.Transaction) error {
	t.ft = ft

	for _, tfunc := range t.tfuncs {
		err := tfunc(ctx, t)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Transaction) Add(docname string, dval interface{}) error {
	dref := t.db.Client.Doc(docname)
	if dref == nil {
		return fmt.Errorf("nil dref: bad docname '%s'?", docname)
	}

	err := t.ft.Create(dref, dval)
	if err != nil {
		return err
	}

	t.db.log.Debugf("docname %s dval %#v", docname, dval)

	return nil
}

func (t *Transaction) AddOrReplace(docname string, dval interface{}) error {
	dref := t.db.Client.Doc(docname)

	err := t.ft.Set(dref, dval)
	if err != nil {
		return err
	}

	t.db.log.Debugf("docname %s", docname)

	return nil
}

func (t *Transaction) Delete(docname string) error {
	dref := t.db.Client.Doc(docname)

	err := t.ft.Delete(dref)
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Get(docname string, dval interface{}) error {
	dref := t.db.Client.Doc(docname)

	dsnap, err := t.ft.Get(dref)
	if err != nil {
		return err
	}

	return dsnap.DataTo(dval)
}

func (t *Transaction) Escape(raw string) string {
	return t.db.Escape(raw)
}

func (t *Transaction) DocumentIterator(colname string) *DocumentIterator {
	col := t.db.Client.Collection(colname)

	iter := t.ft.Documents(col)

	return &DocumentIterator{iter}
}

func (t *Transaction) QueryIterator(colname string, attr string, comparison string, val string) *DocumentIterator {
	col := t.db.Client.Collection(colname)

	query := col.Where(attr, comparison, val)

	iter := t.ft.Documents(query)

	return &DocumentIterator{iter}
}

func (t *Transaction) NextDocPath(iter *DocumentIterator, dval interface{}) (string, error) {
	dsnap, err := iter.Next()
	if err == iterator.Done {
		return "", DBIteratorDone
	}
	if err != nil {
		return "", err
	}

	if dval != nil {
		err = dsnap.DataTo(dval)
		if err != nil {
			return "", err
		}
	}

	return dsnap.Ref.Path, nil
}

type DBCreateFunc func(ctx context.Context, dval interface{}) error

func (db *DBConnection) AtomicGetOrCreate(ctx context.Context, docname string, dval interface{}, createfunc DBCreateFunc) error {
	dref := db.Client.Doc(docname)

	txfunc := func(ctx context.Context, tx *firestore.Transaction) error {
		dsnap, err := tx.Get(dref)
		if err == nil {
			dsnap.DataTo(dval)
			return nil
		}

		if !ErrorIsNotFound(err) {
			return err
		}

		err = createfunc(ctx, dval)
		if err != nil {
			return err
		}

		err = tx.Create(dref, dval)
		if err != nil {
			return err
		}

		return nil
	}

	err := db.Client.RunTransaction(ctx, txfunc)

	if err != nil {
		return err
	}

	return nil
}

type DBUpdateFunc func(ctx context.Context, dval interface{}) error

func (db *DBConnection) AtomicUpdate(ctx context.Context, docname string, dval interface{}, updateFunc DBUpdateFunc) error {
	dref := db.Client.Doc(docname)

	txfunc := func(ctx context.Context, tx *firestore.Transaction) error {
		dsnap, err := tx.Get(dref)
		if err != nil {
			return err
		}

		err = dsnap.DataTo(dval)
		if err != nil {
			return err
		}

		err = updateFunc(ctx, dval)
		if err != nil {
			return err
		}

		err = tx.Set(dref, dval)
		if err != nil {
			return err
		}

		return nil
	}

	err := db.Client.RunTransaction(ctx, txfunc)

	if err != nil {
		return err
	}

	return nil
}
