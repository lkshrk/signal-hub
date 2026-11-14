package config

import (
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const (
	accountPath = "/etc/signal-hub/account"
	tokenDir    = "/etc/signal-hub/tokens"
	botNumber   = "+4915112345678"
)

func groupID(fill byte) string {
	b := make([]byte, 32)
	for i := range b {
		b[i] = fill + byte(i)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func restID(internal string) string {
	return "group." + base64.StdEncoding.EncodeToString([]byte(internal))
}

type fixture struct {
	yaml  string
	files map[string]string
	env   map[string]string
	reads []string
}

func newFixture() *fixture {
	return &fixture{
		yaml: `version: 1
upstream:
  url: http://signal-rest-api.tools.svc.cluster.local
  accountFile: ` + accountPath + `
retention: 14d
consumers:
  - name: dsh
    groups: ["` + groupID(0) + `"]
    maxReplayAge: 7d
  - name: hermes
    groups: ["` + groupID(1) + `"]
    senders: ["+4915187654321"]
    compat:
      listen: ":8091"
      attachmentDir: /data/hermes-out
  - name: ops
    groups: ["` + groupID(2) + `"]
`,
		files: map[string]string{
			accountPath:       botNumber + "\n",
			tokenDir + "/dsh": strings.Repeat("a", 32),
			tokenDir + "/ops": strings.Repeat("b", 32) + "\n",
		},
		env: map[string]string{},
	}
}

func (f *fixture) replace(t *testing.T, old, repl string) {
	t.Helper()
	if !strings.Contains(f.yaml, old) {
		t.Fatalf("fixture does not contain %q", old)
	}
	f.yaml = strings.Replace(f.yaml, old, repl, 1)
}

func (f *fixture) parse() (*Config, error) {
	return Parse([]byte(f.yaml), Env{
		Getenv: func(k string) string { return f.env[k] },
		ReadFile: func(p string) ([]byte, error) {
			f.reads = append(f.reads, p)
			s, ok := f.files[p]
			if !ok {
				return nil, &fs.PathError{Op: "open", Path: p, Err: fs.ErrNotExist}
			}
			return []byte(s), nil
		},
	})
}

func (f *fixture) read(p string) bool {
	for _, r := range f.reads {
		if r == p {
			return true
		}
	}
	return false
}

func TestParseExampleConfig(t *testing.T) {
	data, err := os.ReadFile("../../examples/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(data, Env{
		Getenv: func(string) string { return "" },
		ReadFile: func(p string) ([]byte, error) {
			switch p {
			case "/etc/signal-hub/account":
				return []byte("+4915112345678\n"), nil
			case "/etc/signal-hub/tokens/dsh":
				return []byte(strings.Repeat("a", 32)), nil
			}
			return nil, fs.ErrNotExist
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Upstream.ResolvedAccount != "+4915112345678" {
		t.Errorf("ResolvedAccount = %q", cfg.Upstream.ResolvedAccount)
	}
	if cfg.Listen != ":8080" || cfg.AdminListen != ":9090" {
		t.Errorf("listen = %q, admin = %q", cfg.Listen, cfg.AdminListen)
	}
	if time.Duration(cfg.Retention) != 14*24*time.Hour {
		t.Errorf("Retention = %v", time.Duration(cfg.Retention))
	}
	if cfg.Limits.MaxRequestBytes != 26214400 {
		t.Errorf("MaxRequestBytes = %d", cfg.Limits.MaxRequestBytes)
	}
	if len(cfg.Consumers) != 2 {
		t.Fatalf("consumers = %d", len(cfg.Consumers))
	}
	dsh, hermes := cfg.Consumers[0], cfg.Consumers[1]
	if len(dsh.GroupIDs) != 1 || dsh.GroupIDs[0] != "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=" {
		t.Errorf("dsh GroupIDs = %v", dsh.GroupIDs)
	}
	if time.Duration(dsh.MaxReplayAge) != 7*24*time.Hour {
		t.Errorf("dsh MaxReplayAge = %v", time.Duration(dsh.MaxReplayAge))
	}
	if hermes.Compat == nil || hermes.Compat.Listen != ":8091" || hermes.Compat.AttachmentDir != "/data/hermes-out" || hermes.Token != "" {
		t.Errorf("hermes compat = %+v, token = %q", hermes.Compat, hermes.Token)
	}
	if len(hermes.GroupIDs) != 1 || hermes.GroupIDs[0] != "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=" {
		t.Errorf("hermes GroupIDs = %v", hermes.GroupIDs)
	}
}

func TestDefaults(t *testing.T) {
	f := newFixture()
	f.yaml = `version: 1
upstream:
  url: https://signal.example
  account: ` + botNumber + `
consumers:
  - name: dsh
    groups: ["` + groupID(0) + `"]
`
	cfg, err := f.parse()
	if err != nil {
		t.Fatal(err)
	}
	if d := cfg.Database; d.URL != "sqlite:///var/lib/signal-hub/hub.db" || d.Driver != "sqlite" || d.Path != "/var/lib/signal-hub/hub.db" {
		t.Errorf("Database = %+v", d)
	}
	if cfg.TokenDir != "/etc/signal-hub/tokens" {
		t.Errorf("TokenDir = %q", cfg.TokenDir)
	}
	if time.Duration(cfg.Consumers[0].MaxReplayAge) != 24*time.Hour {
		t.Errorf("MaxReplayAge = %v", time.Duration(cfg.Consumers[0].MaxReplayAge))
	}
	if cfg.Listen != ":8080" || cfg.AdminListen != ":9090" {
		t.Errorf("listen = %q, admin = %q", cfg.Listen, cfg.AdminListen)
	}
	if time.Duration(cfg.Retention) != 14*24*time.Hour {
		t.Errorf("Retention = %v", time.Duration(cfg.Retention))
	}
	if cfg.Limits.MaxRequestBytes != 26214400 {
		t.Errorf("MaxRequestBytes = %d", cfg.Limits.MaxRequestBytes)
	}
	if cfg.Consumers[0].Compat != nil {
		t.Errorf("Compat = %+v", cfg.Consumers[0].Compat)
	}
}

func TestAccountSources(t *testing.T) {
	const other = "+4915100000000"
	tests := []struct {
		name         string
		upstream     string
		env          string
		want         string
		wantFileRead bool
	}{
		{"account only", "  account: " + botNumber, "", botNumber, false},
		{"accountFile only", "  accountFile: " + accountPath, "", botNumber, true},
		{"env over accountFile", "  accountFile: " + accountPath, other, other, false},
		{"env over account", "  account: " + botNumber, other, other, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture()
			f.replace(t, "  accountFile: "+accountPath, tt.upstream)
			if tt.env != "" {
				f.env["SIGNAL_HUB_ACCOUNT"] = tt.env
			}
			cfg, err := f.parse()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Upstream.ResolvedAccount != tt.want {
				t.Errorf("ResolvedAccount = %q, want %q", cfg.Upstream.ResolvedAccount, tt.want)
			}
			if f.read(accountPath) != tt.wantFileRead {
				t.Errorf("account file read = %v, want %v", f.read(accountPath), tt.wantFileRead)
			}
		})
	}
}

func TestDurationAcceptsDays(t *testing.T) {
	ok := map[string]time.Duration{
		"7d":  7 * 24 * time.Hour,
		"90s": 90 * time.Second,
		"10m": 10 * time.Minute,
		"24h": 24 * time.Hour,
	}
	for in, want := range ok {
		var d Duration
		if err := yaml.Unmarshal([]byte(in), &d); err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if time.Duration(d) != want {
			t.Errorf("%q = %v, want %v", in, time.Duration(d), want)
		}
	}
	for _, in := range []string{"1.5d", "d", "-1d", "99999999999d", "[1]", "abc"} {
		var d Duration
		if err := yaml.Unmarshal([]byte(in), &d); err == nil {
			t.Errorf("%q: expected error, got %v", in, time.Duration(d))
		}
	}
}

func TestValidationErrors(t *testing.T) {
	restA := restID(groupID(0))
	tests := []struct {
		name   string
		mutate func(t *testing.T, f *fixture)
		want   string
	}{
		{"version", func(t *testing.T, f *fixture) { f.replace(t, "version: 1", "version: 2") }, "version: must be 1"},
		{"url required", func(t *testing.T, f *fixture) {
			f.replace(t, "url: http://signal-rest-api.tools.svc.cluster.local", `url: ""`)
		}, "upstream.url: required"},
		{"url scheme", func(t *testing.T, f *fixture) {
			f.replace(t, "http://signal-rest-api.tools.svc.cluster.local", "ftp://x")
		}, "upstream.url: must be an http or https URL"},
		{"account not E.164", func(t *testing.T, f *fixture) {
			f.replace(t, "accountFile: "+accountPath, "account: \"4915\"")
		}, "upstream.account: must be E.164"},
		{"accountFile not E.164", func(t *testing.T, f *fixture) { f.files[accountPath] = "4915" }, "upstream.accountFile: must be E.164"},
		{"env not E.164", func(t *testing.T, f *fixture) { f.env["SIGNAL_HUB_ACCOUNT"] = "4915" }, "SIGNAL_HUB_ACCOUNT: must be E.164"},
		{"account and accountFile", func(t *testing.T, f *fixture) {
			f.replace(t, "accountFile: "+accountPath, "accountFile: "+accountPath+"\n  account: "+botNumber)
		}, "upstream: set account or accountFile, not both"},
		{"no account", func(t *testing.T, f *fixture) {
			f.replace(t, "  accountFile: "+accountPath+"\n", "")
		}, "upstream: no account; set SIGNAL_HUB_ACCOUNT, upstream.accountFile or upstream.account"},
		{"accountFile missing", func(t *testing.T, f *fixture) { delete(f.files, accountPath) }, "upstream.accountFile: open /etc/signal-hub/account"},
		{"accountFile empty", func(t *testing.T, f *fixture) { f.files[accountPath] = "\n" }, "upstream.accountFile: file is empty"},
		{"no consumers", func(t *testing.T, f *fixture) {
			f.yaml = "version: 1\nupstream:\n  url: http://x\n  account: " + botNumber + "\nconsumers: []\n"
		}, "consumers: at least one consumer is required"},
		{"duplicate name", func(t *testing.T, f *fixture) { f.replace(t, "name: hermes", "name: dsh") }, `consumers[1].name: "dsh" is already used`},
		{"bad name", func(t *testing.T, f *fixture) { f.replace(t, "name: dsh", "name: DSH") }, "consumers[0].name: must match ^[a-z0-9][a-z0-9-]{0,31}$"},
		{"path-like name", func(t *testing.T, f *fixture) { f.replace(t, "name: dsh", "name: ../x") }, "consumers[0].name: must match"},
		{"no groups or senders", func(t *testing.T, f *fixture) {
			f.replace(t, `    groups: ["`+groupID(2)+`"]`, "    groups: []")
		}, "consumers[2]: list at least one group or sender"},
		{"group prefix", func(t *testing.T, f *fixture) {
			f.replace(t, `groups: ["`+groupID(0)+`"]`, `groups: ["group:`+groupID(0)+`"]`)
		}, `drop the "group:" prefix`},
		{"group shared rest and internal", func(t *testing.T, f *fixture) {
			f.replace(t, `groups: ["`+groupID(0)+`"]`, `groups: ["`+restA+`"]`)
			f.replace(t, `groups: ["`+groupID(1)+`"]`, `groups: ["`+groupID(0)+`"]`)
		}, "group " + groupID(0) + ` is listed by consumers "dsh" and "hermes"`},
		{"sender shared", func(t *testing.T, f *fixture) {
			f.replace(t, `    groups: ["`+groupID(0)+`"]`, `    groups: ["`+groupID(0)+`"]`+"\n    senders: [\"+4915187654321\"]")
		}, `sender +4915187654321 is listed by consumers "dsh" and "hermes"`},
		{"bad sender", func(t *testing.T, f *fixture) { f.replace(t, `"+4915187654321"`, `"hello"`) }, "consumers[1].senders[0]: must be an E.164 number or a UUID"},
		{"maxReplayAge over retention", func(t *testing.T, f *fixture) { f.replace(t, "maxReplayAge: 7d", "maxReplayAge: 15d") }, "consumers[0].maxReplayAge: must not exceed retention"},
		{"compat listen clash", func(t *testing.T, f *fixture) { f.replace(t, `listen: ":8091"`, `listen: ":8080"`) }, `consumers[1].compat.listen: ":8080" is already used`},
		{"admin listen clash", func(t *testing.T, f *fixture) {
			f.replace(t, "retention: 14d", "retention: 14d\nadminListen: \":8080\"")
		}, `adminListen: ":8080" is already used`},
		{"compat attachmentDir relative", func(t *testing.T, f *fixture) {
			f.replace(t, "attachmentDir: /data/hermes-out", "attachmentDir: data/hermes-out")
		}, "consumers[1].compat.attachmentDir: must be an absolute path"},
		{"compat listen empty", func(t *testing.T, f *fixture) { f.replace(t, `listen: ":8091"`, `listen: ""`) }, "consumers[1].compat.listen: required"},
		{"unknown key", func(t *testing.T, f *fixture) { f.yaml += "foo: 1\n" }, "field foo not found"},
		{"token missing", func(t *testing.T, f *fixture) { delete(f.files, tokenDir+"/dsh") }, "consumers[0]: token file"},
		{"token short", func(t *testing.T, f *fixture) { f.files[tokenDir+"/dsh"] = "short" }, "consumers[0]: token must be at least 32 characters"},
		{"token shared", func(t *testing.T, f *fixture) { f.files[tokenDir+"/ops"] = f.files[tokenDir+"/dsh"] }, `consumers "dsh" and "ops" share a token`},
		{"database postgres", func(t *testing.T, f *fixture) {
			f.replace(t, "retention: 14d", "retention: 14d\ndatabase:\n  url: postgres://u@h/db")
		}, `database.url: unsupported scheme "postgres"; only sqlite is supported`},
		{"database relative", func(t *testing.T, f *fixture) {
			f.replace(t, "retention: 14d", "retention: 14d\ndatabase:\n  url: sqlite://hub.db")
		}, "database.url: sqlite needs an absolute path"},
		{"database query", func(t *testing.T, f *fixture) {
			f.replace(t, "retention: 14d", "retention: 14d\ndatabase:\n  url: sqlite:///x/hub.db?mode=ro")
		}, "database.url: query parameters are not supported"},
		{"old database path", func(t *testing.T, f *fixture) {
			f.replace(t, "retention: 14d", "retention: 14d\ndatabase: /var/lib/x.db")
		}, "parse config"},
		{"empty document", func(t *testing.T, f *fixture) { f.yaml = "" }, "config is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture()
			tt.mutate(t, f)
			_, err := f.parse()
			if err == nil {
				t.Fatalf("expected error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}

func TestValidBaseConfig(t *testing.T) {
	cfg, err := newFixture().parse()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Consumers[0].Token != strings.Repeat("a", 32) || cfg.Consumers[2].Token != strings.Repeat("b", 32) {
		t.Errorf("tokens not trimmed or read: %q %q", cfg.Consumers[0].Token, cfg.Consumers[2].Token)
	}
	if cfg.Consumers[1].Token != "" {
		t.Errorf("hermes token = %q", cfg.Consumers[1].Token)
	}
}

func TestSendersAreNormalized(t *testing.T) {
	f := newFixture()
	f.replace(t, `"+4915187654321"`, `"0B6A1C2D-1111-2222-3333-444455556666"`)
	cfg, err := f.parse()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Consumers[1].Senders[0]; got != "0b6a1c2d-1111-2222-3333-444455556666" {
		t.Errorf("sender = %q", got)
	}
}

func TestValidationReportsAllErrors(t *testing.T) {
	f := newFixture()
	f.replace(t, "version: 1", "version: 2")
	f.env["SIGNAL_HUB_ACCOUNT"] = "4915"
	_, err := f.parse()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"version: must be 1", "SIGNAL_HUB_ACCOUNT: must be E.164"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestErrorsDoNotLeakSecrets(t *testing.T) {
	f := newFixture()
	f.files[accountPath] = "4915999888777"
	f.files[tokenDir+"/dsh"] = "tiny-secret-token"
	_, err := f.parse()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, leak := range []string{"4915999888777", "tiny-secret-token"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error leaks %q: %v", leak, err)
		}
	}
	f = newFixture()
	f.env["SIGNAL_HUB_ACCOUNT"] = "+49151bad"
	if _, err = f.parse(); err == nil || strings.Contains(err.Error(), "+49151bad") {
		t.Errorf("env error = %v", err)
	}
	f = newFixture()
	f.replace(t, "accountFile: "+accountPath, "account: \"+4915bad\"")
	if _, err = f.parse(); err == nil || strings.Contains(err.Error(), "+4915bad") {
		t.Errorf("account error = %v", err)
	}
}

func TestLoadReadsTokenFiles(t *testing.T) {
	dir := t.TempDir()
	token := strings.Repeat("t", 40)
	if err := os.WriteFile(filepath.Join(dir, "dsh"), []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	body := `version: 1
upstream:
  url: http://x
  account: ` + botNumber + `
tokenDir: ` + dir + `
consumers:
  - name: dsh
    groups: ["` + groupID(0) + `"]
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SIGNAL_HUB_ACCOUNT", "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Consumers[0].Token != token {
		t.Errorf("Token = %q", cfg.Consumers[0].Token)
	}
}

func TestLoadMissingTokenFileWrapsNotExist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `version: 1
upstream:
  url: http://x
  account: ` + botNumber + `
tokenDir: ` + dir + `
consumers:
  - name: dsh
    groups: ["` + groupID(0) + `"]
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err = %v, want wrapping fs.ErrNotExist", err)
	}
}

func TestLoadUsesProcessEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `version: 1
upstream:
  url: http://x
  accountFile: ` + filepath.Join(dir, "nowhere") + `
consumers:
  - name: hermes
    groups: ["` + groupID(0) + `"]
    compat:
      listen: ":8091"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SIGNAL_HUB_ACCOUNT", "+4915100000000")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Upstream.ResolvedAccount != "+4915100000000" {
		t.Errorf("ResolvedAccount = %q", cfg.Upstream.ResolvedAccount)
	}
}

func TestMaxReplayAgeDefaultFollowsRetention(t *testing.T) {
	f := newFixture()
	f.replace(t, "retention: 14d", "retention: 12h")
	f.replace(t, "    maxReplayAge: 7d\n", "")
	cfg, err := f.parse()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cfg.Consumers {
		if time.Duration(c.MaxReplayAge) != 12*time.Hour {
			t.Errorf("%s MaxReplayAge = %v", c.Name, time.Duration(c.MaxReplayAge))
		}
	}

	f = newFixture()
	f.replace(t, "retention: 14d", "retention: 12h")
	_, err = f.parse()
	if err == nil || !strings.Contains(err.Error(), "consumers[0].maxReplayAge: must not exceed retention") {
		t.Errorf("explicit maxReplayAge over retention: err = %v", err)
	}
}

func TestGroupIDsAreDeduplicated(t *testing.T) {
	f := newFixture()
	f.replace(t, `groups: ["`+groupID(0)+`"]`, `groups: ["`+groupID(0)+`", "`+restID(groupID(0))+`"]`)
	cfg, err := f.parse()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Consumers[0].GroupIDs; len(got) != 1 || got[0] != groupID(0) {
		t.Errorf("GroupIDs = %v", got)
	}
}
