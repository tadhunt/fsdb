package fsdb

import (
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
	Data  map[string]any
}

func NewJoinCodeManager(id string) *JoinCodeManager {
	return &JoinCodeManager{
		ID: id,
	}
}

func (jcm *JoinCodeManager) LookupByCode(t *Transaction, code string) (*JoinCode, error) {
	dbpath := fmt.Sprintf("joincodes-bycode/%s", code)

	jc := &JoinCode{}
	err := t.Get(dbpath, jc)
	if err != nil {
		return nil, err
	}

	return jc, nil
}

func (jcm *JoinCodeManager) LookupByUID(t *Transaction, uid string) (*JoinCode, error) {
	dbpath := fmt.Sprintf("joincodes-byname/%s_%s", jcm.ID, uid)

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

func (jcm *JoinCodeManager) Create(t *Transaction, uid string, data map[string]any) (*JoinCode, error) {
	jc := &JoinCode{}

	for i := 0; i < 20; i++ {
		code, err := newJoinCode()
		if err != nil {
			return nil, err
		}

		jc.Code = code
		jc.MgrID = jcm.ID
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
