package groupid

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

const (
	internalA = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	restA     = "group.QUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0="

	internalSlash = "+/+++/+++/+++/+++/+++/+++/+++/+++/+++/++++8="
	internalURL   = "-_---_---_---_---_---_---_---_---_---_----8="
)

func TestNormalizeAcceptsBothForms(t *testing.T) {
	for _, in := range []string{internalA, restA} {
		got, err := Normalize(in)
		if err != nil || got != internalA {
			t.Fatalf("Normalize(%q) = %q, %v; want %q", in, got, err, internalA)
		}
	}
}

func TestRESTRoundTrip(t *testing.T) {
	if got := REST(internalA); got != restA {
		t.Fatalf("REST(internalA) = %q; want %q", got, restA)
	}
	for _, x := range []string{internalA, internalSlash} {
		got, err := Normalize(REST(x))
		if err != nil || got != x {
			t.Fatalf("Normalize(REST(%q)) = %q, %v", x, got, err)
		}
	}
}

func TestStdEncodingOnly(t *testing.T) {
	if got, err := Normalize(internalSlash); err != nil || got != internalSlash {
		t.Fatalf("Normalize(internalSlash) = %q, %v", got, err)
	}
	for _, in := range []string{internalURL, "group." + base64.StdEncoding.EncodeToString([]byte(internalURL))} {
		if _, err := Normalize(in); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Normalize(%q) err = %v; want ErrInvalid", in, err)
		}
	}
}

func TestNormalizeRejectsNonCanonical(t *testing.T) {
	trailingBits := strings.TrimSuffix(internalA, "Hh8=") + "Hh9="
	if raw, err := base64.StdEncoding.DecodeString(trailingBits); err != nil || len(raw) != 32 {
		t.Fatalf("vector must decode leniently to 32 bytes: %v", err)
	}
	for _, id := range []string{internalA + "\n", internalA + "\r\n", "AAEC\nAwQF" + internalA[8:], trailingBits} {
		for _, in := range []string{id, "group." + base64.StdEncoding.EncodeToString([]byte(id)), REST(id)} {
			if _, err := Normalize(in); !errors.Is(err, ErrInvalid) {
				t.Errorf("Normalize(%q) err = %v; want ErrInvalid", in, err)
			}
			if IsGroup(in) {
				t.Errorf("IsGroup(%q) = true; want false", in)
			}
		}
	}
	if _, err := Normalize(restA + "\n"); !errors.Is(err, ErrInvalid) {
		t.Errorf("REST with trailing newline err = %v; want ErrInvalid", err)
	}
}

func TestNormalizeRejects(t *testing.T) {
	cases := []string{
		"",
		"group.",
		"group.!!!",
		base64.StdEncoding.EncodeToString(make([]byte, 16)),
		"+4915112345678",
		"0b6a4f2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b",
		"group:" + internalA,
	}
	for _, in := range cases {
		_, err := Normalize(in)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("Normalize(%q) err = %v; want ErrInvalid", in, err)
		}
	}
	_, err := Normalize("group:" + internalA)
	if err == nil || !strings.Contains(err.Error(), `drop the "group:" prefix`) {
		t.Errorf("group: prefix error = %v; want hint to drop the prefix", err)
	}
}

func TestIsGroup(t *testing.T) {
	for _, in := range []string{internalA, restA} {
		if !IsGroup(in) {
			t.Errorf("IsGroup(%q) = false; want true", in)
		}
	}
	for _, in := range []string{"+4915112345678", "0b6a4f2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b", "group:" + internalA} {
		if IsGroup(in) {
			t.Errorf("IsGroup(%q) = true; want false", in)
		}
	}
}
