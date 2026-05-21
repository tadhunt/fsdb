package fsdb

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/tadhunt/logger"
)

type cacheTestDoc struct {
	Name  string
	Value int
}

// requireTestDB connects to the FSDB_TEST_* database or fails the test.
// Same env-var contract as TestCreateDatabase.
func requireTestDB(t *testing.T) (*DBConnection, context.Context) {
	t.Helper()
	project := os.Getenv("FSDB_TEST_PROJECT")
	db := os.Getenv("FSDB_TEST_DB")
	credentialsFile := os.Getenv("FSDB_TEST_CREDENTIALS_FILE")
	if project == "" {
		t.Fatalf("FSDB_TEST_PROJECT unset")
	}
	if db == "" {
		t.Fatalf("FSDB_TEST_DB unset")
	}
	if credentialsFile == "" {
		t.Fatalf("FSDB_TEST_CREDENTIALS_FILE unset")
	}
	ctx := context.Background()
	log := logger.NewTestCompatLogWriter(t)
	dbc, err := NewDBConnectionWithDatabase(ctx, log, project, db, &Credentials{File: &credentialsFile})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return dbc, ctx
}

// TestTransactionGetCacheHit verifies that two Get calls on the same docname
// inside one transaction produce exactly one underlying Firestore read.
func TestTransactionGetCacheHit(t *testing.T) {
	dbc, ctx := requireTestDB(t)
	collection := "_fsdb_test_" + t.Name()
	docname := collection + "/doc"
	defer dbc.Delete(ctx, docname)

	want := cacheTestDoc{Name: "hi", Value: 42}
	if err := dbc.Add(ctx, docname, want); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got1, got2 cacheTestDoc
	var captured *Transaction
	err := dbc.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
		captured = tx
		if err := tx.Get(docname, &got1); err != nil {
			return err
		}
		return tx.Get(docname, &got2)
	})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	if got1 != want {
		t.Errorf("first Get: got %+v want %+v", got1, want)
	}
	if got2 != want {
		t.Errorf("second Get: got %+v want %+v", got2, want)
	}
	if captured.cacheMisses != 1 {
		t.Errorf("cacheMisses = %d, want 1", captured.cacheMisses)
	}
	if captured.cacheHits != 1 {
		t.Errorf("cacheHits = %d, want 1", captured.cacheHits)
	}
}

// TestTransactionGetCacheNegative verifies that ErrorIsNotFound is cached:
// two Get calls of a non-existent doc produce exactly one underlying read.
func TestTransactionGetCacheNegative(t *testing.T) {
	dbc, ctx := requireTestDB(t)
	docname := "_fsdb_test_" + t.Name() + "/doc"

	var got cacheTestDoc
	var captured *Transaction
	err := dbc.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
		captured = tx
		err1 := tx.Get(docname, &got)
		if !ErrorIsNotFound(err1) {
			return fmt.Errorf("first Get: want NotFound, got %v", err1)
		}
		err2 := tx.Get(docname, &got)
		if !ErrorIsNotFound(err2) {
			return fmt.Errorf("second Get: want NotFound, got %v", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	if captured.cacheMisses != 1 {
		t.Errorf("cacheMisses = %d, want 1", captured.cacheMisses)
	}
	if captured.cacheHits != 1 {
		t.Errorf("cacheHits = %d, want 1", captured.cacheHits)
	}
}

// TestTransactionIteratorPrefill verifies that NextDocPath populates the
// cache, so a later Get on an iterated doc is served without a round-trip.
func TestTransactionIteratorPrefill(t *testing.T) {
	dbc, ctx := requireTestDB(t)
	collection := "_fsdb_test_" + t.Name()

	docnames := []string{
		collection + "/a",
		collection + "/b",
		collection + "/c",
	}
	for i, dn := range docnames {
		if err := dbc.Add(ctx, dn, cacheTestDoc{Name: fmt.Sprintf("d%d", i), Value: i}); err != nil {
			t.Fatalf("seed %s: %v", dn, err)
		}
	}
	defer func() {
		for _, dn := range docnames {
			_ = dbc.Delete(ctx, dn)
		}
	}()

	var got cacheTestDoc
	var captured *Transaction
	err := dbc.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
		captured = tx
		iter := tx.DocumentIterator(collection)
		defer iter.Stop()
		for {
			var d cacheTestDoc
			_, err := tx.NextDocPath(iter, &d)
			if errors.Is(err, DBIteratorDone) {
				break
			}
			if err != nil {
				return err
			}
		}
		// After iteration the cache should be prefilled. This Get should hit.
		return tx.Get(docnames[1], &got)
	})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	if got.Name != "d1" || got.Value != 1 {
		t.Errorf("Get: got %+v want {d1 1}", got)
	}
	if captured.cacheHits < 1 {
		t.Errorf("cacheHits = %d, want >= 1 (iterator prefill should serve Get)", captured.cacheHits)
	}
}

// TestTransactionCacheLRUEvicts verifies the LRU cap by exercising the
// cache helpers directly: insert cap+1 entries, observe the oldest one
// has been evicted (subsequent lookups miss), while the most-recent
// entries are still cached.
func TestTransactionCacheLRUEvicts(t *testing.T) {
	tx := &Transaction{}
	// Simulate handler init without a real Firestore transaction.
	tx.cache = make(map[string]*list.Element, transactionCacheMax)
	tx.cacheOrder = list.New()

	// Fill the cache and one extra. The oldest entry (key0) should be
	// evicted; key1..keyN should remain.
	for i := 0; i <= transactionCacheMax; i++ {
		tx.cacheStore(fmt.Sprintf("k%d", i), cachedRead{})
	}

	if _, ok := tx.cacheLookup("k0"); ok {
		t.Errorf("k0 should have been evicted")
	}
	for i := 1; i <= transactionCacheMax; i++ {
		k := fmt.Sprintf("k%d", i)
		if _, ok := tx.cacheLookup(k); !ok {
			t.Errorf("%s should still be cached", k)
		}
	}
	if tx.cacheOrder.Len() != transactionCacheMax {
		t.Errorf("cacheOrder.Len()=%d, want %d", tx.cacheOrder.Len(), transactionCacheMax)
	}
	if len(tx.cache) != transactionCacheMax {
		t.Errorf("len(cache)=%d, want %d", len(tx.cache), transactionCacheMax)
	}

	// Touch k1 to make it most-recent, then push a fresh entry. Now k2
	// should be the LRU victim, not k1.
	tx.cacheLookup("k1")
	tx.cacheStore("fresh", cachedRead{})
	if _, ok := tx.cacheLookup("k2"); ok {
		t.Errorf("k2 should have been evicted (k1 was touched to move it forward)")
	}
	if _, ok := tx.cacheLookup("k1"); !ok {
		t.Errorf("k1 should still be cached")
	}
}

// TestTransactionCacheIsPerTransaction verifies the cache resets between
// separate RunTransaction calls — each transaction gets a fresh view.
func TestTransactionCacheIsPerTransaction(t *testing.T) {
	dbc, ctx := requireTestDB(t)
	docname := "_fsdb_test_" + t.Name() + "/doc"
	defer dbc.Delete(ctx, docname)
	if err := dbc.Add(ctx, docname, cacheTestDoc{Name: "x", Value: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for i := 0; i < 2; i++ {
		var got cacheTestDoc
		var captured *Transaction
		err := dbc.RunTransaction(ctx, func(ctx context.Context, tx *Transaction) error {
			captured = tx
			return tx.Get(docname, &got)
		})
		if err != nil {
			t.Fatalf("RunTransaction %d: %v", i, err)
		}
		if captured.cacheMisses != 1 || captured.cacheHits != 0 {
			t.Errorf("tx %d: cacheMisses=%d cacheHits=%d, want 1/0",
				i, captured.cacheMisses, captured.cacheHits)
		}
	}
}
