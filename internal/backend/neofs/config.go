package neofs

import (
	"net/url"
	"strings"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to neofs.
type Config struct {
	Endpoint  string
	Container string

	Wallet            string        `option:"wallet" help:"path to the wallet"`
	Address           string        `option:"address" help:"address of account (can be empty)"`
	Password          string        `option:"password" help:"password to decrypt wallet"`
	Timeout           time.Duration `option:"timeout" help:"timeout to connect and request (default 10s)"`
	RebalanceInterval time.Duration `option:"rebalance" help:"interval between checking node healthy (default 15s)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{}
}

func init() {
	options.Register("neofs", Config{})
}

// ParseConfig parses the string s and extracts the neofs config.
// The configuration format is neofs:grpcs://s01.neofs.devenv:8080/container,
// where 'container' is container name or container id.
func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "neofs:") {
		return nil, errors.New("neofs: invalid format")
	}

	// strip prefix "neofs:"
	s = s[6:]
	u, err := url.Parse(s)
	if err != nil {
		return nil, errors.Wrap(err, "url.Parse")
	}

	cfg := NewConfig()
	cfg.Container = strings.TrimPrefix(u.Path, "/")
	cfg.Endpoint = strings.TrimSuffix(s, u.Path)
	return cfg, nil
}
