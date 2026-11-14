package testupstream

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func dial(t *testing.T, s *Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, s.URL(), nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func read(t *testing.T, c *websocket.Conn) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	return b, err
}

func TestSessionsRunInOrderAndLastRepeats(t *testing.T) {
	s := New(t, Write([]byte("one")), Seq(Write([]byte("two")), Drop))
	for i, want := range []string{"one", "two", "two"} {
		if i == 0 {
			c := dial(t, s)
			if b, err := read(t, c); err != nil || string(b) != want {
				t.Fatalf("dial %d: %q, %v", i, b, err)
			}
			_ = c.CloseNow()
			continue
		}
		c := dial(t, s)
		if b, err := read(t, c); err != nil || string(b) != want {
			t.Fatalf("dial %d: %q, %v", i, b, err)
		}
		if _, err := read(t, c); err == nil {
			t.Fatalf("dial %d: connection should drop", i)
		}
	}
	if s.Dials() != 3 {
		t.Fatalf("dials = %d", s.Dials())
	}
}

func TestHoldRecordsCloseCode(t *testing.T) {
	s := New(t)
	c := dial(t, s)
	if err := c.Close(websocket.StatusGoingAway, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-s.Closed():
		if code != websocket.StatusGoingAway {
			t.Fatalf("code = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no close recorded")
	}
}

func TestStallNeverAnswersPings(t *testing.T) {
	s := New(t, Stall)
	c := dial(t, s)
	go func() { _, _, _ = c.Read(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Ping(ctx); err == nil {
		t.Fatal("ping should time out")
	}
}

func TestHandleHTTPServesPlainRequestsBesideTheSocket(t *testing.T) {
	s := New(t, Write([]byte("frame")))
	s.HandleHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("rest " + r.URL.Path))
	}))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.HTTPURL()+"/v1/about", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if string(body) != "rest /v1/about" {
		t.Fatalf("body = %q", body)
	}
	if b, err := read(t, dial(t, s)); err != nil || string(b) != "frame" {
		t.Fatalf("socket: %q, %v", b, err)
	}
	if s.Dials() != 1 {
		t.Fatalf("dials = %d, want 1", s.Dials())
	}
}
