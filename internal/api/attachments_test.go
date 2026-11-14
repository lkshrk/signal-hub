package api

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAttachmentStreamsForOwnMessage(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "direct_attachment", baseTime.Add(-time.Hour))
	payload := bytes.Repeat([]byte("jpeg"), 1000)
	f.upstream.set(http.StatusOK, payload)

	res := f.get(t, dshToken, "/v1/attachments/x1y2.jpg")
	body := res.Body
	if res.StatusCode != 200 || !bytes.Equal(body, payload) {
		t.Fatalf("%d, %d bytes", res.StatusCode, len(body))
	}
	if ct := res.Header.Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("content-type = %q", ct)
	}
	if cl := res.Header.Get("Content-Length"); cl != "4000" {
		t.Errorf("content-length = %q", cl)
	}
	if cd := res.Header.Get("Content-Disposition"); !strings.Contains(cd, "shot.jpg") {
		t.Errorf("content-disposition = %q", cd)
	}
}

func TestAttachmentOfOtherConsumerIs404(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "direct_attachment", baseTime.Add(-time.Hour))
	f.upstream.set(http.StatusOK, []byte("x"))

	other := f.get(t, hermesToken, "/v1/attachments/x1y2.jpg")
	otherBody := string(other.Body)
	unknown := f.get(t, hermesToken, "/v1/attachments/nope.jpg")
	unknownBody := string(unknown.Body)
	if other.StatusCode != 404 || unknown.StatusCode != 404 || otherBody != unknownBody {
		t.Fatalf("other=%d %q unknown=%d %q", other.StatusCode, otherBody, unknown.StatusCode, unknownBody)
	}
	if f.upstream.count() != 0 {
		t.Fatalf("upstream contacted %d times", f.upstream.count())
	}
}

func TestAttachmentRequiresAuth(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "direct_attachment", baseTime.Add(-time.Hour))
	if res := f.get(t, "", "/v1/attachments/x1y2.jpg"); res.StatusCode != 401 {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestAttachmentUpstreamErrorIs502(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "direct_attachment", baseTime.Add(-time.Hour))
	f.upstream.set(http.StatusInternalServerError, nil)

	res := f.get(t, dshToken, "/v1/attachments/x1y2.jpg")
	body := string(res.Body)
	if res.StatusCode != 502 || strings.Contains(body, botNumber) {
		t.Fatalf("%d %s", res.StatusCode, body)
	}
	if logs := f.logs.String(); strings.Contains(logs, botNumber) || !strings.Contains(logs, `"consumer":"dsh"`) {
		t.Fatalf("logs: %s", logs)
	}
}

func TestAttachmentFallsBackToStoredSize(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "direct_attachment", baseTime.Add(-time.Hour))
	payload := bytes.Repeat([]byte("j"), 48213)
	f.upstream.set(http.StatusOK, payload)
	f.upstream.mu.Lock()
	f.upstream.noLen = true
	f.upstream.mu.Unlock()

	res := f.get(t, dshToken, "/v1/attachments/x1y2.jpg")
	if res.StatusCode != 200 || res.Header.Get("Content-Length") != "48213" || len(res.Body) != 48213 {
		t.Fatalf("%d cl=%q len=%d", res.StatusCode, res.Header.Get("Content-Length"), len(res.Body))
	}
}
