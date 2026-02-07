package fsdb

import (
	"context"

	"cloud.google.com/go/firestore"
)

// Direction is the sort direction for OrderBy clauses.
type Direction = firestore.Direction

const (
	// Asc sorts results in ascending order.
	Asc = firestore.Asc
	// Desc sorts results in descending order.
	Desc = firestore.Desc
)

// Query is a fluent query builder for Firestore collections.
// Build a query by chaining methods, then call Documents to execute it.
//
// Example:
//
//	iter := db.Query("users").
//		Where("age", ">=", 18).
//		OrderBy("age", fsdb.Asc).
//		Limit(10).
//		Documents(ctx)
type Query struct {
	query firestore.Query
	tx    *firestore.Transaction
}

// Query creates a new query builder for the named collection.
func (db *DBConnection) Query(colname string) *Query {
	return &Query{
		query: db.Client.Collection(colname).Query,
	}
}

// QueryGroup creates a new query builder for a collection group.
// A collection group includes all collections with the given ID,
// regardless of their parent document.
func (db *DBConnection) QueryGroup(colname string) *Query {
	return &Query{
		query: db.Client.CollectionGroup(colname).Query,
	}
}

// Query creates a new query builder for the named collection within a transaction.
func (t *Transaction) Query(colname string) *Query {
	return &Query{
		query: t.db.Client.Collection(colname).Query,
		tx:    t.ft,
	}
}

// QueryGroup creates a new query builder for a collection group within a transaction.
func (t *Transaction) QueryGroup(colname string) *Query {
	return &Query{
		query: t.db.Client.CollectionGroup(colname).Query,
		tx:    t.ft,
	}
}

// Where adds a filter condition to the query.
// The op argument must be one of "==", "!=", "<", "<=", ">", ">=",
// "array-contains", "array-contains-any", "in", or "not-in".
func (q *Query) Where(path, op string, value interface{}) *Query {
	q.query = q.query.Where(path, op, value)
	return q
}

// OrderBy adds a sort ordering to the query.
// Multiple OrderBy calls can be chained; they are applied in order.
func (q *Query) OrderBy(path string, dir Direction) *Query {
	q.query = q.query.OrderBy(path, dir)
	return q
}

// Limit sets the maximum number of results to return.
func (q *Query) Limit(n int) *Query {
	q.query = q.query.Limit(n)
	return q
}

// LimitToLast sets the maximum number of results to return from the end
// of the ordered result set. Requires at least one OrderBy clause.
func (q *Query) LimitToLast(n int) *Query {
	q.query = q.query.LimitToLast(n)
	return q
}

// Offset sets the number of results to skip before returning results.
func (q *Query) Offset(n int) *Query {
	q.query = q.query.Offset(n)
	return q
}

// StartAt sets the query cursor to start at the given field values (inclusive).
// The values must correspond to the OrderBy fields, in the same order.
func (q *Query) StartAt(values ...interface{}) *Query {
	q.query = q.query.StartAt(values...)
	return q
}

// StartAfter sets the query cursor to start after the given field values (exclusive).
// The values must correspond to the OrderBy fields, in the same order.
func (q *Query) StartAfter(values ...interface{}) *Query {
	q.query = q.query.StartAfter(values...)
	return q
}

// EndAt sets the query cursor to end at the given field values (inclusive).
// The values must correspond to the OrderBy fields, in the same order.
func (q *Query) EndAt(values ...interface{}) *Query {
	q.query = q.query.EndAt(values...)
	return q
}

// EndBefore sets the query cursor to end before the given field values (exclusive).
// The values must correspond to the OrderBy fields, in the same order.
func (q *Query) EndBefore(values ...interface{}) *Query {
	q.query = q.query.EndBefore(values...)
	return q
}

// Select specifies which document fields to return.
// If no fields are specified, only document references are returned.
func (q *Query) Select(fields ...string) *Query {
	q.query = q.query.Select(fields...)
	return q
}

// Documents executes the query and returns a DocumentIterator over the results.
func (q *Query) Documents(ctx context.Context) *DocumentIterator {
	if q.tx != nil {
		return &DocumentIterator{q.tx.Documents(q.query)}
	}
	return &DocumentIterator{q.query.Documents(ctx)}
}
