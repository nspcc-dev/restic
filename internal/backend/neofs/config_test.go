package neofs

import "testing"

func TestParseConfig(t *testing.T) {
	for i, test := range []struct {
		s   string
		cfg Config
	}{
		{"neofs:grpcs://s01.neofs.devenv:8080/container-name", Config{
			Endpoint:    "grpcs://s01.neofs.devenv:8080",
			Container:   "container-name",
			Connections: 5,
		}},
	} {
		cfg, err := ParseConfig(test.s)
		if err != nil {
			t.Errorf("test %d:%s failed: %v", i, test.s, err)
			continue
		}

		if cfg != test.cfg {
			t.Errorf("test %d:\ninput:\n  %s\n wrong config, want:\n  %v\ngot:\n  %v",
				i, test.s, test.cfg, cfg)
			continue
		}
	}
}
