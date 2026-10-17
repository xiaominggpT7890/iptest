package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
)

// TaskConfig 是一个完整任务的快照配置
type TaskConfig struct {
	Proxy     ProxyConfig `json:"proxy"`
	TargetURL string      `json:"target_url"`
	Sessid    SessidRange `json:"sessid_range"`

	DurationSeconds  int `json:"duration_seconds"`
	Concurrency      int `json:"concurrency"`
	RequestTimeoutMs int `json:"request_timeout_ms"`

	PoolSweep       PoolSweepConfig       `json:"pool_sweep"`
	StickinessProbe StickinessProbeConfig `json:"stickiness_probe"`
}

// ProxyConfig 接受 "USER:PASS@HOST:PORT" 模板字符串,
// USER 中必须含 {num} 占位符,每次请求替换为 sessid。
type ProxyConfig struct {
	Template string `json:"template"`
	Scheme   string `json:"scheme,omitempty"`

	UserTemplate string `json:"-"`
	Password     string `json:"-"`
	Host         string `json:"-"`
	Port         int    `json:"-"`
}

type SessidRange struct {
	Min  int64  `json:"min"`
	Max  int64  `json:"max"`
	Mode string `json:"mode"`
}

const (
	SessidRandom     = "random"
	SessidSequential = "sequential"
)

type PoolSweepConfig struct {
	Enabled           bool    `json:"enabled"`
	StopWhenHitRateGE float64 `json:"stop_when_hit_rate_above"`
	HitRateWindowSize int     `json:"hit_rate_window_size"`
}

type StickinessProbeConfig struct {
	Enabled             bool  `json:"enabled"`
	SessidCount         int   `json:"sessid_count"`
	PollScheduleSeconds []int `json:"poll_schedule_seconds"`
	LoopSchedule        bool  `json:"loop_schedule"`
}

func (c *TaskConfig) Validate() error {
	c.Proxy.Template = normalizeProxyTemplate(c.Proxy.Template)
	if c.Proxy.Scheme == "" {
		c.Proxy.Scheme = "socks5"
	}
	if c.Proxy.Template == "" {
		return errors.New("proxy template required")
	}
	if !strings.Contains(c.Proxy.Template, "{num}") {
		return errors.New("proxy template must contain {num} placeholder for sessid")
	}
	userTpl, pwd, host, port, err := ParseProxyTemplate(c.Proxy.Template)
	if err != nil {
		return fmt.Errorf("parse proxy template: %w", err)
	}
	c.Proxy.UserTemplate = userTpl
	c.Proxy.Password = pwd
	c.Proxy.Host = host
	c.Proxy.Port = port

	if c.TargetURL == "" {
		c.TargetURL = "https://api.skyvora.cc/appapi/geo/cityinfo"
	}
	if c.Sessid.Min == 0 && c.Sessid.Max == 0 {
		c.Sessid.Min, c.Sessid.Max = 1_000_000, 9_000_000
	}
	if c.Sessid.Mode == "" {
		c.Sessid.Mode = SessidRandom
	}
	if c.Sessid.Mode != SessidRandom && c.Sessid.Mode != SessidSequential {
		return errors.New("sessid_range.mode must be random or sequential")
	}
	if c.Sessid.Min < 0 || c.Sessid.Max < c.Sessid.Min || c.Sessid.Max-c.Sessid.Min == math.MaxInt64 {
		return errors.New("sessid_range must be nonnegative, ordered and fit in int64")
	}
	if c.Sessid.Mode == SessidSequential && c.Sessid.Max == math.MaxInt64 {
		return errors.New("sequential sessid_range.max must be less than int64 maximum")
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 200
	}
	if c.Concurrency > 30000 {
		return errors.New("concurrency cannot exceed 30000")
	}
	if c.RequestTimeoutMs <= 0 {
		c.RequestTimeoutMs = 15000
	}
	if !c.PoolSweep.Enabled && !c.StickinessProbe.Enabled {
		return errors.New("at least one of pool_sweep/stickiness_probe must be enabled")
	}
	if c.PoolSweep.Enabled && c.PoolSweep.HitRateWindowSize <= 0 {
		c.PoolSweep.HitRateWindowSize = 1000
	}
	if c.StickinessProbe.Enabled {
		if c.StickinessProbe.SessidCount <= 0 {
			c.StickinessProbe.SessidCount = 50
		}
		if int64(c.StickinessProbe.SessidCount) > c.Sessid.Max-c.Sessid.Min+1 {
			return errors.New("stickiness_probe.sessid_count cannot exceed sessid range capacity")
		}
		if len(c.StickinessProbe.PollScheduleSeconds) == 0 {
			c.StickinessProbe.PollScheduleSeconds = []int{60}
			c.StickinessProbe.LoopSchedule = true
		}
		for _, v := range c.StickinessProbe.PollScheduleSeconds {
			if v < 1 {
				return fmt.Errorf("poll_schedule_seconds must be >= 1, got %d", v)
			}
		}
	}
	return nil
}

// ParseProxyTemplate 解析 "USER:PASS@HOST:PORT"
func ParseProxyTemplate(s string) (userTpl, password, host string, port int, err error) {
	s = normalizeProxyTemplate(s)
	for _, p := range []string{"socks5h://", "socks5://", "socks://"} {
		if strings.HasPrefix(s, p) {
			s = s[len(p):]
			break
		}
	}
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return "", "", "", 0, errors.New(`missing "@" between credentials and host`)
	}
	creds := s[:at]
	hostport := s[at+1:]

	colon := strings.LastIndex(creds, ":")
	if colon < 0 {
		return "", "", "", 0, errors.New(`missing ":" between username and password`)
	}
	userTpl = creds[:colon]
	password = creds[colon+1:]

	h, p, err2 := net.SplitHostPort(hostport)
	if err2 != nil {
		return "", "", "", 0, fmt.Errorf("invalid host:port %q: %w", hostport, err2)
	}
	portN, err2 := strconv.Atoi(p)
	if err2 != nil || portN <= 0 || portN > 65535 {
		return "", "", "", 0, fmt.Errorf("invalid port %q", p)
	}
	return userTpl, password, h, portN, nil
}

func normalizeProxyTemplate(s string) string {
	s = strings.TrimSpace(s)
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	colon := strings.LastIndex(s[at+1:], ":")
	if colon < 0 {
		return s[:at+1] + strings.TrimSpace(s[at+1:])
	}
	colon += at + 1
	return s[:at+1] + strings.TrimSpace(s[at+1:colon]) + ":" + strings.TrimSpace(s[colon+1:])
}

func ParseTaskConfig(b []byte) (*TaskConfig, error) {
	c := &TaskConfig{}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *TaskConfig) ToJSON() []byte {
	b, _ := json.Marshal(c)
	return b
}
