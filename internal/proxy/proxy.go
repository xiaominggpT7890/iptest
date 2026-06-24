package proxy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"
)

type GeoResult struct {
	IP      string
	Country string
	Region  string
	City    string
	Raw     string
}

const (
	ErrTimeout      = "timeout"
	ErrTLSHandshake = "tls_handshake"
	ErrProxyDead    = "proxy_dead"
	ErrProxyBlocked = "proxy_blocked"
	ErrUpstream5xx  = "upstream_5xx"
	ErrUpstream4xx  = "upstream_4xx"
	ErrGeoEmpty     = "geo_empty"
	ErrCanceled     = "canceled"
	ErrConnReset    = "conn_reset"
	ErrEOF          = "eof"
	ErrParse        = "parse_err"
	ErrOther        = "other"
)

type FetchError struct {
	Kind string
	Msg  string
}

func (e *FetchError) Error() string { return e.Kind + ": " + e.Msg }

// Builder 构造代理请求用的客户端
//   USER 模板中的 {num} 占位符在每次请求时替换为 sessid。
type Builder struct {
	Scheme       string
	UserTemplate string
	Password     string
	Host         string
	Port         int
	Timeout      time.Duration
}

func (b *Builder) proxyURL(sessid int64) (*url.URL, error) {
	username := strings.ReplaceAll(b.UserTemplate, "{num}", strconv.FormatInt(sessid, 10))
	scheme := b.Scheme
	if scheme == "" {
		scheme = "socks5"
	}
	return &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(username, b.Password),
		Host:   fmt.Sprintf("%s:%d", b.Host, b.Port),
	}, nil
}

// NewClient 为指定 sessid 构造一个 http.Client。
// 分阶段超时:
//   - SOCKS5 dial: 5s
//   - TLS handshake: 5s
//   - HTTP response: 10s
//   - 总 client.Timeout: b.Timeout (兜底)
func (b *Builder) NewClient(sessid int64) (*http.Client, error) {
	pu, err := b.proxyURL(sessid)
	if err != nil {
		return nil, err
	}
	dialer, err := xproxy.FromURL(pu, xproxy.Direct)
	if err != nil {
		return nil, err
	}
	ctxDialer, ok := dialer.(xproxy.ContextDialer)
	if !ok {
		return nil, errors.New("proxy dialer is not ContextDialer")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return ctxDialer.DialContext(dialCtx, network, addr)
		},
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConnsPerHost:   2,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   b.Timeout,
	}, nil
}

func Fetch(ctx context.Context, client *http.Client, targetURL string) (*GeoResult, *FetchError) {
	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return nil, &FetchError{Kind: ErrOther, Msg: err.Error()}
	}
	req.Header.Set("User-Agent", "iptest/1.0")
	req.Header.Set("Accept", "application/json,*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, classifyErr(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return nil, &FetchError{Kind: ErrUpstream5xx, Msg: fmt.Sprintf("status=%d", resp.StatusCode)}
	}
	if resp.StatusCode >= 400 {
		return nil, &FetchError{Kind: ErrUpstream4xx, Msg: fmt.Sprintf("status=%d", resp.StatusCode)}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, classifyErr(err)
	}

	if !looksLikeJSON(body) {
		return nil, &FetchError{Kind: ErrProxyBlocked, Msg: truncate(string(body), 200)}
	}
	g, perr := parseGeo(body)
	if perr != nil {
		if perr == errGeoEmpty {
			return nil, &FetchError{Kind: ErrGeoEmpty, Msg: truncate(string(body), 200)}
		}
		return nil, &FetchError{Kind: ErrParse, Msg: truncate(perr.Error()+" body="+string(body), 200)}
	}
	return g, nil
}

// classifyErr 把 http.Client.Do 返回的 error 精细分类
func classifyErr(err error) *FetchError {
	msg := err.Error()
	low := strings.ToLower(msg)
	short := truncate(msg, 200)

	switch {
	case errors.Is(err, context.Canceled), strings.Contains(low, "context canceled"):
		return &FetchError{Kind: ErrCanceled, Msg: short}
	case errors.Is(err, context.DeadlineExceeded), strings.Contains(low, "deadline exceeded"),
		strings.Contains(low, "tls handshake timeout"), strings.Contains(low, "i/o timeout"):
		return &FetchError{Kind: ErrTimeout, Msg: short}
	case strings.Contains(low, "tls: first record does not look like a tls handshake"),
		strings.Contains(low, "unexpected protocol version"):
		return &FetchError{Kind: ErrProxyDead, Msg: short}
	case strings.Contains(low, "remote error: tls"),
		strings.Contains(low, "failed to parse certificate"),
		strings.Contains(low, "x509"):
		return &FetchError{Kind: ErrTLSHandshake, Msg: short}
	case strings.Contains(low, "connection refused"),
		strings.Contains(low, "no route to host"),
		strings.Contains(low, "network is unreachable"):
		return &FetchError{Kind: ErrProxyDead, Msg: short}
	case strings.Contains(low, "connection reset by peer"),
		strings.Contains(low, "broken pipe"):
		return &FetchError{Kind: ErrConnReset, Msg: short}
	case strings.HasSuffix(low, "eof"), strings.Contains(low, "unexpected eof"):
		return &FetchError{Kind: ErrEOF, Msg: short}
	case strings.Contains(low, "socks"):
		return &FetchError{Kind: ErrProxyDead, Msg: short}
	default:
		return &FetchError{Kind: ErrOther, Msg: short}
	}
}

func looksLikeJSON(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

var errGeoEmpty = errors.New("geo: empty ip")

func parseGeo(body []byte) (*GeoResult, error) {
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	if d, ok := raw["data"].(map[string]interface{}); ok {
		raw = d
	}
	g := &GeoResult{Raw: string(body)}
	g.IP = pickStr(raw, "ip", "query", "ipAddress", "ipaddress")
	g.Country = pickStr(raw, "isocode", "countryCode", "country_code", "cc", "country")
	g.Region = pickStr(raw, "region", "regionName", "province", "state", "timezone")
	g.City = pickStr(raw, "city", "cityName")
	if g.IP == "" {
		return nil, errGeoEmpty
	}
	return g, nil
}

func pickStr(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
