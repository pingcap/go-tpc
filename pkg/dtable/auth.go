package dtable

import (
	"time"

	"github.com/dgrijalva/jwt-go"
)

type claims struct {
	Exp        int64  `json:"exp"`
	DtableUUID string `json:"dtable_uuid"`
	UserName   string `json:"username"`
	IsDBAdmin  bool   `json:"is_db_admin"`
	From       string `json:"from"`
	Permission string `json:"permission"`
	TableId    string `json:"table_id"`
	Internal   bool   `json:"is_internal"`
}

func (claims *claims) Valid() error {
	return nil
}

func newDtableJWT(privateKey string, uuid string) (string, error) {
	now := time.Now()
	exp := now.Add(time.Hour * 1)
	jwtToken := jwt.NewWithClaims(jwt.GetSigningMethod("HS256"), &claims{
		Exp:        exp.Unix(),
		UserName:   "admin@seafile.com",
		Permission: "rw",
		DtableUUID: uuid,
		Internal:   true,
	})

	token, err := jwtToken.SignedString([]byte(privateKey))
	if err != nil {
		return "", err
	}

	return token, nil
}
