package fsdb

import (
	"container/list"
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

type TransactionFunc func(ctx context.Context, t *Transaction) error

// cachedRead memoizes a single Firestore document read within a transaction.
// A non-nil err (e.g. ErrorIsNotFound) is cached just like a successful read.
type cachedRead struct {
	snap *firestore.DocumentSnapshot
	err  error
}

type cacheEntry struct {
	docpath string
	value   cachedRead
}

// transactionCacheMax caps the per-transaction read cache. Sized so that
// worst-case memory under high concurrency stays within a Cloud Run
// instance's typical RAM: ~50KB/doc × 64 entries × 80 concurrent ≈ 250MB.
const transactionCacheMax = 64

type Transaction struct {
	db     *DBConnection
	ft     *firestore.Transaction
	tfuncs []TransactionFunc

	// Per-attempt LRU read cache keyed by DocumentRef.Path. Firestore
	// disallows reads after writes within a transaction, so writes only
	// need to invalidate defensively. Reset on every handler invocation
	// so retries see a fresh view.
	cache       map[string]*list.Element
	cacheOrder  *list.List
	cacheHits   int
	cacheMisses int
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
	t.cache = make(map[string]*list.Element, transactionCacheMax)
	t.cacheOrder = list.New()
	t.cacheHits = 0
	t.cacheMisses = 0

	for _, tfunc := range t.tfuncs {
		err := tfunc(ctx, t)
		if err != nil {
			return err
		}
	}

	return nil
}

// cacheLookup returns the cached read for docpath and marks it most-recent.
func (t *Transaction) cacheLookup(docpath string) (cachedRead, bool) {
	elem, ok := t.cache[docpath]
	if !ok {
		return cachedRead{}, false
	}
	t.cacheOrder.MoveToFront(elem)
	return elem.Value.(*cacheEntry).value, true
}

// cacheStore inserts or refreshes a cache entry, evicting the LRU entry if
// the cache is over capacity.
func (t *Transaction) cacheStore(docpath string, val cachedRead) {
	if elem, ok := t.cache[docpath]; ok {
		elem.Value.(*cacheEntry).value = val
		t.cacheOrder.MoveToFront(elem)
		return
	}
	elem := t.cacheOrder.PushFront(&cacheEntry{docpath: docpath, value: val})
	t.cache[docpath] = elem

	for t.cacheOrder.Len() > transactionCacheMax {
		oldest := t.cacheOrder.Back()
		if oldest == nil {
			break
		}
		delete(t.cache, oldest.Value.(*cacheEntry).docpath)
		t.cacheOrder.Remove(oldest)
	}
}

// cacheEvict removes an entry from the cache if present.
func (t *Transaction) cacheEvict(docpath string) {
	if elem, ok := t.cache[docpath]; ok {
		t.cacheOrder.Remove(elem)
		delete(t.cache, docpath)
	}
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

	t.cacheEvict(dref.Path)
	t.db.log.Debugf("docname %s dval %#v", docname, dval)

	return nil
}

func (t *Transaction) AddOrReplace(docname string, dval interface{}) error {
	dref := t.db.Client.Doc(docname)

	err := t.ft.Set(dref, dval)
	if err != nil {
		return err
	}

	t.cacheEvict(dref.Path)
	t.db.log.Debugf("docname %s", docname)

	return nil
}

func (t *Transaction) Delete(docname string) error {
	dref := t.db.Client.Doc(docname)

	err := t.ft.Delete(dref)
	if err != nil {
		return err
	}

	t.cacheEvict(dref.Path)
	return nil
}

func (t *Transaction) Get(docname string, dval interface{}) error {
	dref := t.db.Client.Doc(docname)
	if dref == nil {
		return fmt.Errorf("nil dref: bad docname '%s'?", docname)
	}

	if c, ok := t.cacheLookup(dref.Path); ok {
		t.cacheHits++
		if c.err != nil {
			return c.err
		}
		return c.snap.DataTo(dval)
	}
	t.cacheMisses++

	dsnap, err := t.ft.Get(dref)
	t.cacheStore(dref.Path, cachedRead{snap: dsnap, err: err})
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

func (t *Transaction) CompoundQueryIterator(colname string, wheres []*DbWhere) *DocumentIterator {
	col := t.db.Client.Collection(colname)

	var query firestore.Query
	for i, w := range wheres {
		if i == 0 {
			query = col.Where(w.Attr, w.Comparison, w.Val)
		} else {
			query = query.Where(w.Attr, w.Comparison, w.Val)
		}
	}

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

	t.cacheStore(dsnap.Ref.Path, cachedRead{snap: dsnap})

	if dval != nil {
		err = dsnap.DataTo(dval)
		if err != nil {
			return "", err
		}
	}

	return dsnap.Ref.Path, nil
}

func (t *Transaction) DeleteCollection(path string) error {
	col := t.db.Client.Collection(path)
	iter := t.ft.Documents(col.Select())
	defer iter.Stop()

	docIDs, err := iter.GetAll()
	if err != nil {
		return err
	}
	for _, doc := range docIDs {
		err := t.ft.Delete(doc.Ref)
		if err != nil {
			return err
		}
		t.cacheEvict(doc.Ref.Path)
	}

	return nil
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
