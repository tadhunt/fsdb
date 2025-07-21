package fsdb

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/firestore"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/tadhunt/logger"
	"github.com/tadhunt/retry"
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

type Credentials struct {
	File          *string
	JSON          []byte
	AccessTokenFile *string
}

func NewDBConnection(ctx context.Context, log logger.CompatLogWriter, project string, credentials *Credentials) (*DBConnection, error) {
	options := []option.ClientOption{}
	if credentials.File != nil {
		options = append(options, option.WithCredentialsFile(*credentials.File))
	} else if credentials.JSON != nil {
		options = append(options, option.WithCredentialsJSON(credentials.JSON))
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

func NewDBConnectionWithDatabase(ctx context.Context, log logger.CompatLogWriter, project string, dbID string, credentials *Credentials) (*DBConnection, error) {
	options := []option.ClientOption{}
	if credentials.File != nil {
		options = append(options, option.WithCredentialsFile(*credentials.File))
	} else if credentials.JSON != nil {
		options = append(options, option.WithCredentialsJSON(credentials.JSON))
	}

	client, err := firestore.NewClientWithDatabase(ctx, project, dbID, options...)
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

func CreateDatabase(ctx context.Context, log logger.CompatLogWriter, project string, dbID string, credentials *Credentials) ([]string, error) {
	exists, err := dbExists(ctx, log, project, dbID, credentials)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("database " + dbID + " already exists")
	}

	path, err := exec.LookPath("gcloud")
	if err != nil {
		return nil, err
	}

	if credentials.AccessTokenFile == nil {
		return nil, errors.New("missing access token file")
	}

	cmd := exec.Command(path,
		"--access-token-file=" + *credentials.AccessTokenFile,
		"--project=" + project,
		"firestore",
		"databases",
		"create",
		"--database=" + dbID,
		"--location=nam5",
		"--type=firestore-native",
	)

	output, err := cmd.CombinedOutput()
	lines := strings.Split(string(output), "\n")

	if err != nil {
		return lines, err
	}

	exists, err = dbExistsRetry(ctx, log, project, dbID, credentials)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("database " + dbID + " creation failed or is delayed")
	}

	return lines, err
}

func dbExistsRetry(ctx context.Context, log logger.CompatLogWriter, project string, dbID string, credentials *Credentials) (bool, error) {
	var errFound = errors.New("found")
	var errNotFound = errors.New("not found")

	retrier := retry.NewRetrier(5, 0, 2*time.Second)

	err := retrier.RunContext(ctx, func(ctx context.Context) error {
		found, err := dbExists(ctx, log, project, dbID, credentials)
		if err != nil {
			return retry.Stop(err)
		}
		if !found {
			return errNotFound
		}

		return retry.Stop(errFound)
	})

	if errors.Is(err, errFound) {
		return true, nil 
	}

	return false, err
}

func dbExists(ctx context.Context, log logger.CompatLogWriter, project string, dbID string, credentials *Credentials) (bool, error) {
	c, err := NewDBConnectionWithDatabase(ctx, log, project, dbID, credentials)
	if err != nil {
		return false, err
	}

	nFound := 0
	iter := c.Client.Collections(ctx)
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break // No more collections
		}
		if err != nil {
			log.Debugf("%v", err)
			errstr := fmt.Sprintf("%v", err)
			match := fmt.Sprintf("database %s does not exist", dbID)
			if strings.Contains(errstr, match) {
				return false, nil
			}
			return false, err
		}
		nFound++
	}

	return true, nil
}
