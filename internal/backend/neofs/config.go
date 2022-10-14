package neofs

import (
	"strings"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to neofs.
type Config struct {
	Container   string
	Compression bool

	Endpoints         string        `option:"endpoints" help:"neofs endpoints, format: '<address> [<priority> [<weight>]]; ...'"`
	Wallet            string        `option:"wallet" help:"path to the wallet"`
	Address           string        `option:"address" help:"address of account (can be empty)"`
	Password          string        `option:"password" help:"password to decrypt wallet"`
	Timeout           time.Duration `option:"timeout" help:"timeout to connect and request (default 10s)"`
	RebalanceInterval time.Duration `option:"rebalance" help:"interval between checking node healthy (default 20s)"`

	Connections uint `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 5,
	}
}

func init() {
	options.Register("neofs", Config{})
}

// ParseConfig parses the string s and extracts the neofs config.
// The configuration format is neofs:container,
// where 'container' is container name or container id.
func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "neofs:") {
		return nil, errors.New("neofs: invalid format")
	}

	// strip prefix "neofs:"
	s = s[6:]

	cfg := NewConfig()
	cfg.Container = s
	return cfg, nil
}
