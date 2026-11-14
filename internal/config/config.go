package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/lkshrk/signal-hub/internal/groupid"
)

const (
	accountEnv      = "SIGNAL_HUB_ACCOUNT"
	minTokenLen     = 32
	defaultListen   = ":8080"
	defaultAdmin    = ":9090"
	defaultDatabase = "/var/lib/signal-hub/hub.db"
	defaultTokenDir = "/etc/signal-hub/tokens"
	defaultMaxBody  = 26214400
	defaultRetain   = 14 * 24 * time.Hour
	defaultReplay   = 24 * time.Hour
)

var (
	e164Re = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
)

type Config struct {
	Version     int        `yaml:"version"`
	Upstream    Upstream   `yaml:"upstream"`
	Listen      string     `yaml:"listen"`
	AdminListen string     `yaml:"adminListen"`
	Database    string     `yaml:"database"`
	Retention   Duration   `yaml:"retention"`
	TokenDir    string     `yaml:"tokenDir"`
	Limits      Limits     `yaml:"limits"`
	Consumers   []Consumer `yaml:"consumers"`
}

type Upstream struct {
	URL             string `yaml:"url"`
	Account         string `yaml:"account"`
	AccountFile     string `yaml:"accountFile"`
	ResolvedAccount string `yaml:"-"`
}

type Limits struct {
	MaxRequestBytes int64 `yaml:"maxRequestBytes"`
}

type Consumer struct {
	Name         string   `yaml:"name"`
	Groups       []string `yaml:"groups"`
	Senders      []string `yaml:"senders"`
	MaxReplayAge Duration `yaml:"maxReplayAge"`
	Compat       *Compat  `yaml:"compat"`
	GroupIDs     []string `yaml:"-"`
	Token        string   `yaml:"-"`
}

type Compat struct {
	Listen string `yaml:"listen"`
}

type Env struct {
	Getenv   func(key string) string
	ReadFile func(path string) ([]byte, error)
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data, Env{Getenv: os.Getenv, ReadFile: os.ReadFile})
}

func Parse(data []byte, env Env) (*Config, error) {
	if env.Getenv == nil {
		env.Getenv = os.Getenv
	}
	if env.ReadFile == nil {
		env.ReadFile = os.ReadFile
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("config is empty")
		}
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	errs := cfg.resolveAccount(env)
	errs = append(errs, cfg.validate(env)...)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	setStr := func(p *string, def string) {
		if *p == "" {
			*p = def
		}
	}
	setStr(&c.Listen, defaultListen)
	setStr(&c.AdminListen, defaultAdmin)
	setStr(&c.Database, defaultDatabase)
	setStr(&c.TokenDir, defaultTokenDir)
	if c.Retention == 0 {
		c.Retention = Duration(defaultRetain)
	}
	if c.Limits.MaxRequestBytes == 0 {
		c.Limits.MaxRequestBytes = defaultMaxBody
	}
	for i := range c.Consumers {
		if c.Consumers[i].MaxReplayAge == 0 {
			c.Consumers[i].MaxReplayAge = Duration(defaultReplay)
		}
	}
}

func readError(path string, err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return err
	}
	return fmt.Errorf("open %s: %w", path, err)
}

func (c *Config) resolveAccount(env Env) []error {
	u := &c.Upstream
	set := func(source, v string) []error {
		if !e164Re.MatchString(v) {
			return []error{fmt.Errorf("%s: must be E.164", source)}
		}
		u.ResolvedAccount = v
		return nil
	}
	var errs []error
	if u.Account != "" && u.AccountFile != "" {
		errs = append(errs, errors.New("upstream: set account or accountFile, not both"))
	}
	switch v := strings.TrimSpace(env.Getenv(accountEnv)); {
	case v != "":
		return append(errs, set(accountEnv, v)...)
	case len(errs) > 0:
		return errs
	case u.AccountFile != "":
		raw, err := env.ReadFile(u.AccountFile)
		if err != nil {
			return []error{fmt.Errorf("upstream.accountFile: %w", readError(u.AccountFile, err))}
		}
		v := strings.TrimSpace(string(raw))
		if v == "" {
			return []error{errors.New("upstream.accountFile: file is empty")}
		}
		return set("upstream.accountFile", v)
	case u.Account != "":
		return set("upstream.account", strings.TrimSpace(u.Account))
	}
	return []error{errors.New("upstream: no account; set SIGNAL_HUB_ACCOUNT, upstream.accountFile or upstream.account")}
}

func (c *Config) validate(env Env) []error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.Version != 1 {
		fail("version: must be 1")
	}
	if c.Upstream.URL == "" {
		fail("upstream.url: required")
	} else if u, err := url.Parse(c.Upstream.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		fail("upstream.url: must be an http or https URL")
	}
	if c.Retention < 0 {
		fail("retention: must be positive")
	}
	if c.Limits.MaxRequestBytes < 0 {
		fail("limits.maxRequestBytes: must be positive")
	}

	listens := map[string]bool{c.Listen: true}
	if listens[c.AdminListen] {
		fail("adminListen: %q is already used", c.AdminListen)
	}
	listens[c.AdminListen] = true

	if len(c.Consumers) == 0 {
		fail("consumers: at least one consumer is required")
	}
	names := map[string]bool{}
	groupOwner := map[string]string{}
	senderOwner := map[string]string{}
	tokenOwner := map[string]string{}
	for i := range c.Consumers {
		con := &c.Consumers[i]
		pre := fmt.Sprintf("consumers[%d]", i)

		nameOK := nameRe.MatchString(con.Name)
		switch {
		case !nameOK:
			fail("%s.name: must match %s", pre, nameRe)
		case names[con.Name]:
			fail("%s.name: %q is already used", pre, con.Name)
		}
		names[con.Name] = true

		if len(con.Groups) == 0 && len(con.Senders) == 0 {
			fail("%s: list at least one group or sender", pre)
		}
		con.GroupIDs = con.GroupIDs[:0]
		for j, g := range con.Groups {
			id, err := groupid.Normalize(g)
			if err != nil {
				fail("%s.groups[%d]: %v", pre, j, err)
				continue
			}
			con.GroupIDs = append(con.GroupIDs, id)
			if owner, ok := groupOwner[id]; ok && owner != con.Name {
				fail("group %s is listed by consumers %q and %q", id, owner, con.Name)
			} else if !ok {
				groupOwner[id] = con.Name
			}
		}
		for j, s := range con.Senders {
			switch {
			case e164Re.MatchString(s):
			case uuidRe.MatchString(s):
				s = strings.ToLower(s)
				con.Senders[j] = s
			default:
				fail("%s.senders[%d]: must be an E.164 number or a UUID", pre, j)
				continue
			}
			if owner, ok := senderOwner[s]; ok && owner != con.Name {
				fail("sender %s is listed by consumers %q and %q", s, owner, con.Name)
			} else if !ok {
				senderOwner[s] = con.Name
			}
		}

		switch {
		case con.MaxReplayAge < 0:
			fail("%s.maxReplayAge: must be positive", pre)
		case con.MaxReplayAge > c.Retention:
			fail("%s.maxReplayAge: must not exceed retention", pre)
		}

		if con.Compat != nil {
			switch l := con.Compat.Listen; {
			case l == "":
				fail("%s.compat.listen: required", pre)
			case listens[l]:
				fail("%s.compat.listen: %q is already used", pre, l)
			default:
				listens[l] = true
			}
		}

		if !nameOK {
			continue
		}
		raw, err := env.ReadFile(filepath.Join(c.TokenDir, con.Name))
		switch {
		case errors.Is(err, fs.ErrNotExist) && con.Compat != nil:
		case err != nil:
			fail("%s: token file: %w", pre, readError(filepath.Join(c.TokenDir, con.Name), err))
		default:
			con.Token = strings.TrimSpace(string(raw))
		}
		switch {
		case con.Token == "" && con.Compat != nil:
		case err != nil:
		case len(con.Token) < minTokenLen:
			fail("%s: token must be at least %d characters", pre, minTokenLen)
		default:
			if owner, ok := tokenOwner[con.Token]; ok {
				fail("consumers %q and %q share a token", owner, con.Name)
			} else {
				tokenOwner[con.Token] = con.Name
			}
		}
	}
	return errs
}
