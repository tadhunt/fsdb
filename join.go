package fsdb

import (
	"context"
	"bytes"
	"crypto/rand"
	"fmt"
)
const (
	JoinCodeLength     = 6
	JoinCodeCharacters = "0123456789"
)

type JoinCodeManager struct {
	ID string
}

type JoinCode struct {
	Code  string
	MgrID string
	UID   string
	Data  any
}

func NewJoinCodeManager(id string) *JoinCodeManager {
	return &JoinCodeManager{
		ID: id,
	}
}

func (jcm *JoinCodeManager) LookupByCode(ctx context.Context, db *DBConnection, code string, data *any) error {
	dbpath := fmt.Sprintf("joincodes-bycode/%s", code)

	jc := &JoinCode{}
	err := db.Get(ctx, dbpath, jc)
	if err != nil {
		if ErrorIsNotFound(err) {
			return nil
		}
		return err
	}

	*data = jc.Data

	return nil
}

func (jcm *JoinCodeManager) LookupByUID(ctx context.Context, db DBConnection, uid string) (*JoinCode, error) {
	dbpath := fmt.Sprintf("joincodes-byname/%s_%s", jcm.ID, uid)

	jc := &JoinCode{}
	err := db.Get(ctx, dbpath, jc)
	if err != nil {
		if ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return jc, nil
}

func (jcm *JoinCodeManager) LookupOrCreate(ctx context.Context, db *DBConnection, uid string) (*JoinCode, error) {
	jc := &JoinCode{}

	err := db.RunTransaction(ctx, func(ctx context.Context, t *Transaction) error {
		byNamePath := fmt.Sprintf("joincodes-byname/%s_%s", jcm.ID, uid)

		err := t.Get(byNamePath, jc)
		if err == nil {
			return nil
		}
		if !ErrorIsNotFound(err) {
			return err
		}

		for i := 0; i < 20; i++ {
			code, err := newJoinCode()
			if err != nil {
				return err
			}

			jc.Code = code
			jc.MgrID = jcm.ID
			jc.UID = uid

			byCodePath := fmt.Sprintf("joincodes-bycode/%s", jc.Code)

			err = t.Add(byCodePath, jc)
			if err != nil {
				if ErrorIsAlreadyExists(err) {
					continue
				}
				return err
			}

			err = t.Add(byNamePath, jc)
			if err != nil {
				return err
			}
		}
		return fmt.Errorf("failed to generate a unique join code")
	})
	if err != nil {
		return nil, err
	}

	return jc, nil
}

func (jcm *JoinCodeManager) JoinCodeDeleteByUID(ctx context.Context, db *DBConnection, uid string) error {
	err := db.RunTransaction(ctx, func(ctx context.Context, t *Transaction) error {
		byNamePath := fmt.Sprintf("joincodes-byname/%s_%s", jcm.ID, uid)

		jc := &JoinCode{}
		err := t.Get(byNamePath, jc)
		if err != nil {
			if ErrorIsNotFound(err) {
				return nil
			}
			return err
		}

		dbpaths := []string{
			byNamePath,
			fmt.Sprintf("joincodes-bycode/%s", jc.Code),
		}

		for _, path := range dbpaths {
			err := t.Delete(path)
			if err != nil {
				if ErrorIsNotFound(err) {
					continue
				}
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (jcm *JoinCodeManager) JoinCodeDelete(ctx context.Context, db *DBConnection, jc *JoinCode) error {
	err := db.RunTransaction(ctx, func(ctx context.Context, t *Transaction) error {
		dbpaths := []string{
			fmt.Sprintf("joincodes-byname/%s_%s", jc.MgrID, jc.UID),
			fmt.Sprintf("joincodes-bycode/%s", jc.Code),
		}

		for _, path := range dbpaths {
			err := t.Delete(path)
			if err != nil {
				if ErrorIsNotFound(err) {
					continue
				}
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func newJoinCode() (string, error) {
	b := make([]byte, JoinCodeLength)

	n, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	if n != len(b) {
		return "", fmt.Errorf("short read got %d expected %d", n, len(b))
	}

	var buf bytes.Buffer
	for _, v := range b {
		buf.WriteByte(JoinCodeCharacters[int(v)%len(JoinCodeCharacters)])
	}

	return buf.String(), nil
}
