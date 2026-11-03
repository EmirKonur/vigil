package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"vigil/internal/probeapi"
)

// Client talks to a Vigil server over HTTP.
type Client struct {
	BaseURL   string
	Token     string
	UserAgent string
	HTTP      *http.Client
}

// NewClient returns a client for the server at baseURL.
func NewClient(baseURL, token, userAgent string) *Client {
	return &Client{
		BaseURL:   strings.TrimSuffix(baseURL, "/"),
		Token:     token,
		UserAgent: userAgent,
		// No client-wide timeout: lease calls long-poll. Each call sets a
		// deadline on its context instead.
		HTTP: &http.Client{},
	}
}

// Lease implements probeapi.API.
func (c *Client) Lease(ctx context.Context, req probeapi.LeaseRequest) (probeapi.LeaseResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, req.Wait+15*time.Second)
	defer cancel()
	var resp probeapi.LeaseResponse
	return resp, c.post(ctx, probeapi.LeasePath, req, &resp)
}

// Report implements probeapi.API.
func (c *Client) Report(ctx context.Context, req probeapi.ReportRequest) (probeapi.ReportResponse, error) {
	var resp probeapi.ReportResponse
	return resp, c.post(ctx, probeapi.ReportPath, req, &resp)
}

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("User-Agent", c.UserAgent)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("server rejected the probe token (check VIGIL_PROBE_TOKEN): %s", strings.TrimSpace(string(msg)))
		}
		return fmt.Errorf("%s %s: %s: %s", http.MethodPost, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
