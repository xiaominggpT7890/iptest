package config

import (
	"math"
	"strings"
	"testing"
)

func TestParseProxyTemplateEndpointWhitespace(t *testing.T) {
	for _, tt := range []struct {
		name, template, host string
	}{
		{"port space", "user-{num}:pass@gw1.example.com: 15015", "gw1.example.com"},
		{"clipboard whitespace", "\tuser-{num}:pass@ gw1.example.com :\u00a015015 \n", "gw1.example.com"},
		{"scheme", " socks5://user-{num}:pass@gw1.example.com: 15015 ", "gw1.example.com"},
		{"ipv6", "user-{num}:pass@ [2001:db8::1] :\t15015", "2001:db8::1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			user, password, host, port, err := ParseProxyTemplate(tt.template)
			if err != nil || user != "user-{num}" || password != "pass" || host != tt.host || port != 15015 {
				t.Fatalf("parse result: user=%q password=%q host=%q port=%d err=%v", user, password, host, port, err)
			}
		})
	}
}

func TestSessidModesAndProbeCapacity(t *testing.T) {
	for _, tt := range []struct {
		name       string
		rangeValue SessidRange
		probeCount int
		valid      bool
	}{
		{"legacy defaults random", SessidRange{Min: 1, Max: 10}, 0, true},
		{"random", SessidRange{Min: 1, Max: 10, Mode: SessidRandom}, 0, true},
		{"sequential", SessidRange{Min: 1, Max: 10, Mode: SessidSequential}, 0, true},
		{"single number", SessidRange{Min: 7, Max: 7, Mode: SessidSequential}, 1, true},
		{"unknown mode", SessidRange{Min: 1, Max: 10, Mode: "ordered"}, 0, false},
		{"negative number", SessidRange{Min: -1, Max: 10}, 0, false},
		{"inverted range", SessidRange{Min: 10, Max: 1}, 0, false},
		{"cursor overflow", SessidRange{Min: 1, Max: math.MaxInt64, Mode: SessidSequential}, 0, false},
		{"range size overflow", SessidRange{Min: 0, Max: math.MaxInt64}, 0, false},
		{"probe capacity", SessidRange{Min: 1, Max: 3}, 3, true},
		{"probe exceeds capacity", SessidRange{Min: 1, Max: 3}, 4, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &TaskConfig{
				Proxy:  ProxyConfig{Template: "test-{num}:pass@127.0.0.1:9102"},
				Sessid: tt.rangeValue, PoolSweep: PoolSweepConfig{Enabled: true},
				StickinessProbe: StickinessProbeConfig{Enabled: tt.probeCount > 0, SessidCount: tt.probeCount},
			}
			err := cfg.Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v, error=%v", tt.valid, err)
			}
			if tt.valid && tt.rangeValue.Mode == "" && cfg.Sessid.Mode != SessidRandom {
				t.Fatalf("legacy task mode=%q, want random", cfg.Sessid.Mode)
			}
		})
	}
}

func TestValidatePersistsNormalizedTemplateWithoutChangingCredentials(t *testing.T) {
	cfg := &TaskConfig{
		Proxy:     ProxyConfig{Template: "user-{num}:p a@ss@ gw1.example.com : 15015 "},
		PoolSweep: PoolSweepConfig{Enabled: true},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Proxy.Template != "user-{num}:p a@ss@gw1.example.com:15015" || cfg.Proxy.Password != "p a@ss" {
		t.Fatalf("unexpected normalized template or credentials: %+v", cfg.Proxy)
	}
	reloaded, err := ParseTaskConfig(cfg.ToJSON())
	if err != nil || reloaded.Proxy.Port != 15015 || reloaded.Proxy.Password != "p a@ss" {
		t.Fatalf("normalized config cannot reload: err=%v", err)
	}
}

func TestParseProxyTemplateStillRejectsInvalidPorts(t *testing.T) {
	for _, port := range []string{"", "0", "65536", "15 015", "15015x", "-1"} {
		t.Run(port, func(t *testing.T) {
			_, _, _, _, err := ParseProxyTemplate("user-{num}:pass@gw1.example.com: " + port)
			if err == nil || !strings.Contains(err.Error(), "invalid port") {
				t.Fatalf("invalid port %q accepted: %v", port, err)
			}
		})
	}
}
