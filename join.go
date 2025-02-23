package fsdb

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
)
const (
	JoinCodeLength     = 6
	JoinCodeCharacters = "0123456789"
)

type JoinCode struct {
	Code  string
	MgrID string
	UID   string
	Data  map[string]string
}

func JoinCodeLookupByCode(t *Transaction, code string) (*JoinCode, error) {
	dbpath := fmt.Sprintf("joincodes-bycode/%s", code)

	jc := &JoinCode{}
	err := t.Get(dbpath, jc)
	if err != nil {
		return nil, err
	}

	return jc, nil
}

func JoinCodeLookupByUID(mgrid string, t *Transaction, uid string) (*JoinCode, error) {
	dbpath := fmt.Sprintf("joincodes-byname/%s_%s", mgrid, uid)

	jc := &JoinCode{}
	err := t.Get(dbpath, jc)
	if err != nil {
		if ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return jc, nil
}

func JoinCodeCreate(mgrid string, t *Transaction, uid string, data map[string]string) (*JoinCode, error) {
	jc := &JoinCode{}

	for i := 0; i < 20; i++ {
		code, err := newJoinCode()
		if err != nil {
			return nil, err
		}

		jc.Code = code
		jc.MgrID = mgrid
		jc.UID = uid
		jc.Data = data

		byCodePath := fmt.Sprintf("joincodes-bycode/%s", jc.Code)

		tmpjc := &JoinCode{}
		err = t.Get(byCodePath, tmpjc)
		if err != nil {
			if ErrorIsNotFound(err) {
				return jc, nil
			}
			return nil, err
		}

		// duplicate code: try again with another code
	}

	return nil, fmt.Errorf("failed to generate a unique join code")
}

func (jc *JoinCode) Save(t *Transaction) error {
	paths := []string{
		fmt.Sprintf("joincodes-byname/%s_%s", jc.MgrID, jc.UID),
		fmt.Sprintf("joincodes-bycode/%s", jc.Code),
	}

	for _, path := range paths {
		err := t.AddOrReplace(path, jc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (jc *JoinCode) Delete(t *Transaction) error {
	paths := []string{
		fmt.Sprintf("joincodes-byname/%s_%s", jc.MgrID, jc.UID),
		fmt.Sprintf("joincodes-bycode/%s", jc.Code),
	}

	for _, path := range paths {
		err := t.Delete(path)
		if err != nil {
			return err
		}
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


func ListJoinCodes(t *Transaction) ([]*JoinCode, error) {
	joincodes := make([]*JoinCode, 0)

	iter := t.DocumentIterator("joincodes-bycode")

	defer iter.Stop()
	for {
		jc := &JoinCode{}
		_, err := t.NextDocPath(iter, jc)
		if err != nil {
			if errors.Is(err, DBIteratorDone) {
				break
			}
			return nil, err
		}

		joincodes = append(joincodes, jc)
	}

	return joincodes, nil
}
