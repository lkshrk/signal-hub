package groupid

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	restPrefix  = "group."
	colonPrefix = "group:"
	idBytes     = 32
)

var ErrInvalid = errors.New("invalid group id")

func Normalize(id string) (string, error) {
	if strings.HasPrefix(id, colonPrefix) {
		return "", fmt.Errorf("%w: drop the \"group:\" prefix", ErrInvalid)
	}
	if rest, ok := strings.CutPrefix(id, restPrefix); ok {
		raw, err := base64.StdEncoding.Strict().DecodeString(rest)
		if err != nil || base64.StdEncoding.EncodeToString(raw) != rest {
			return "", fmt.Errorf("%w: REST form is not base64", ErrInvalid)
		}
		id = string(raw)
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(id)
	if err != nil || len(raw) != idBytes || base64.StdEncoding.EncodeToString(raw) != id {
		return "", fmt.Errorf("%w: expected base64 of %d bytes", ErrInvalid, idBytes)
	}
	return id, nil
}

func REST(internal string) string {
	return restPrefix + base64.StdEncoding.EncodeToString([]byte(internal))
}

func IsGroup(recipient string) bool {
	_, err := Normalize(recipient)
	return err == nil
}
