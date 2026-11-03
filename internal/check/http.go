package check

import (
	"bytes"
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
)

// HTTP checks an HTTP(S) endpoint.
type HTTP struct {
	URL             string            `yaml:"url" json:"url"`
	Method          string            `yaml:"method" json:"method"`
	Headers         map[string]string `yaml:"headers" json:"headers,omitempty"`
	Body            string            `yaml:"body" json:"body,omitempty"`
	FollowRedirects *bool             `yaml:"follow_redirects" json:"follow_redirects,omitempty"`
	MaxRedirects    int               `yaml:"max_redirects" json:"max_redirects"`
	// Insecure skips certificate verification. Expiry is still reported.
	Insecure     bool       `yaml:"insecure" json:"insecure,omitempty"`
	CertWarnDays int        `yaml:"cert_warn_days" json:"cert_warn_days"`
	Expect       HTTPExpect `yaml:"expect" json:"expect"`
}

// HTTPExpect lists the assertions applied to a response.
type HTTPExpect struct {
	// Status accepts exact codes ("204") and classes ("2xx").
	Status       []string        `yaml:"status" json:"status,omitempty"`
	BodyContains string          `yaml:"body_contains" json:"body_contains,omitempty"`
	JSON         []JSONAssertion `yaml:"json" json:"json,omitempty"`
	MaxLatency   time.Duration   `yaml:"max_latency" json:"max_latency,omitempty"`
}

// JSONAssertion checks a value in a JSON response body. Path is dot
// separated; numeric segments index arrays ("checks.0.state").
type JSONAssertion struct {
	Path   string  `yaml:"path" json:"path"`
	Equals *string `yaml:"equals" json:"equals,omitempty"`
}

// maxBody bounds how much of a response is read for keyword and JSON
// assertions. Health endpoints are small; a 1 GiB response is a bug in the
// target, and the probe should not be the one that runs out of memory.
const maxBody = 1 << 20

func (r *Runner) runHTTP(ctx context.Context, h *HTTP) Result {
	body := io.Reader(http.NoBody)
	if h.Body != "" {
		body = strings.NewReader(h.Body)
	}
	req, err := http.NewRequestWithContext(ctx, h.Method, h.URL, body)
	if err != nil {
		return Result{Error: err.Error()}
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" && r.UserAgent != "" {
		req.Header.Set("User-Agent", r.UserAgent)
	}

	client := &http.Client{
		// A fresh transport per check, without keep-alives: reusing a
		// pooled connection would skip DNS, TCP, and TLS on most runs and
		// hide exactly the failures an uptime check exists to catch.
		Transport: &http.Transport{
			Proxy:             http.ProxyFromEnvironment,
			DialContext:       (&net.Dialer{}).DialContext,
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: h.Insecure, RootCAs: r.RootCAs},
			DisableKeepAlives: true,
		},
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if h.FollowRedirects != nil && !*h.FollowRedirects {
				return http.ErrUseLastResponse
			}
			limit := h.MaxRedirects
			if limit <= 0 {
				limit = 5
			}
			if len(via) > limit {
				return fmt.Errorf("stopped after %d redirects", limit)
			}
			return nil
		},
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return Result{Error: describe(err), Latency: time.Since(start)}
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	latency := time.Since(start)

	res := Result{StatusCode: resp.StatusCode, Latency: latency}
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		res.CertNotAfter = resp.TLS.PeerCertificates[0].NotAfter
		warnDays := h.CertWarnDays
		if warnDays == 0 {
			warnDays = 14
		}
		if w, expired := certWarning(res.CertNotAfter, r.now(), warnDays); expired {
			res.Error = w
			return res
		} else if w != "" {
			res.Warning = w
		}
	}
	if readErr != nil {
		res.Error = "reading body: " + describe(readErr)
		return res
	}
	res.Error = h.Expect.verify(resp.StatusCode, data, latency)
	res.OK = res.Error == ""
	return res
}

func (e HTTPExpect) verify(status int, body []byte, latency time.Duration) string {
	statuses := e.Status
	if len(statuses) == 0 {
		statuses = []string{"2xx"}
	}
	if !statusMatches(statuses, status) {
		return fmt.Sprintf("status %d, want %s", status, strings.Join(statuses, " or "))
	}
	if e.BodyContains != "" && !bytes.Contains(body, []byte(e.BodyContains)) {
		return fmt.Sprintf("body does not contain %q", e.BodyContains)
	}
	if len(e.JSON) > 0 {
		var doc any
		if err := json.Unmarshal(body, &doc); err != nil {
			return "body is not valid JSON"
		}
		for _, a := range e.JSON {
			if msg := a.verify(doc); msg != "" {
				return msg
			}
		}
	}
	if e.MaxLatency > 0 && latency > e.MaxLatency {
		return fmt.Sprintf("slow response: %s, limit %s", latency.Round(time.Millisecond), e.MaxLatency)
	}
	return ""
}

func statusMatches(patterns []string, status int) bool {
	code := strconv.Itoa(status)
	for _, p := range patterns {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == code {
			return true
		}
		if len(p) == 3 && strings.HasSuffix(p, "xx") && p[0] == code[0] {
			return true
		}
	}
	return false
}

func (a JSONAssertion) verify(doc any) string {
	v, ok := lookup(doc, a.Path)
	if !ok {
		return fmt.Sprintf("json %s: missing", a.Path)
	}
	if a.Equals == nil {
		return ""
	}
	if got := scalarString(v); got != *a.Equals {
		return fmt.Sprintf("json %s = %s, want %s", a.Path, got, *a.Equals)
	}
	return ""
}

func lookup(doc any, path string) (any, bool) {
	cur := doc
	for _, seg := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return "null"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}
