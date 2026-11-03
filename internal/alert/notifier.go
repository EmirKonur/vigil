package alert

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	"vigil/internal/config"
)

// Notifier delivers one message to one destination.
type Notifier interface {
	Send(ctx context.Context, m Message) error
}

// PermanentError marks a failure that retrying will not fix, such as a
// webhook URL that returns 404.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// New builds a notifier from configuration.
func New(cfg config.Notifier, userAgent string) Notifier {
	if cfg.Type == "email" {
		return &Email{cfg: cfg}
	}
	return &Webhook{cfg: cfg, client: &http.Client{Timeout: 15 * time.Second}, userAgent: userAgent}
}

// Webhook posts JSON to a URL, shaped by preset.
type Webhook struct {
	cfg       config.Notifier
	client    *http.Client
	userAgent string
}

// Send posts the message.
func (w *Webhook) Send(ctx context.Context, m Message) error {
	body, err := Payload(w.cfg.Preset, m)
	if err != nil {
		return &PermanentError{err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return &PermanentError{err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", w.userAgent)
	req.Header.Set("X-Vigil-Delivery", m.DeliveryID)
	req.Header.Set("X-Vigil-Event", string(m.Kind))
	if w.cfg.Secret != "" {
		mac := hmac.New(sha256.New, []byte(w.cfg.Secret))
		mac.Write(body)
		req.Header.Set("X-Vigil-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	err = fmt.Errorf("webhook returned %s", resp.Status)
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusGone:
		return &PermanentError{err}
	}
	return err
}

// Payload encodes m for a webhook preset.
func Payload(preset string, m Message) ([]byte, error) {
	title, body := Title(m), Body(m)
	switch preset {
	case "slack":
		blocks := []map[string]any{
			{"type": "section", "text": map[string]string{"type": "mrkdwn", "text": "*" + title + "*\n" + body}},
		}
		if m.URL != "" {
			blocks = append(blocks, map[string]any{
				"type":     "context",
				"elements": []map[string]string{{"type": "mrkdwn", "text": "<" + m.URL + "|Open in Vigil>"}},
			})
		}
		// text is the fallback for notifications and screen readers.
		return json.Marshal(map[string]any{"text": title, "blocks": blocks})
	case "discord":
		embed := map[string]any{
			"title":       title,
			"description": body,
			"color":       discordColor(m.Kind),
			"timestamp":   m.At.UTC().Format(time.RFC3339),
		}
		if m.URL != "" {
			embed["url"] = m.URL
		}
		return json.Marshal(map[string]any{"username": "Vigil", "embeds": []any{embed}})
	default:
		m.Schema = Schema
		return json.Marshal(struct {
			Message
			Title string `json:"title"`
			Text  string `json:"text"`
		}{m, title, body})
	}
}

// discordColor maps alert kinds onto the status palette.
func discordColor(k Kind) int {
	switch k {
	case KindResolved:
		return 0x2F7D4A
	case KindFlapping, KindCertExpiring:
		return 0xB7791F
	default:
		return 0xC43C25
	}
}

// Email sends plain-text mail over SMTP.
type Email struct{ cfg config.Notifier }

// Send delivers the message to every recipient in one transaction.
func (e *Email) Send(ctx context.Context, m Message) error {
	c := e.cfg.SMTP
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	var err error
	if c.TLS == "tls" {
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: &tls.Config{ServerName: c.Host}}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer client.Close()

	if c.TLS == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return &PermanentError{errors.New("smtp server does not offer STARTTLS; set smtp.tls to none to send in the clear")}
		}
		if err := client.StartTLS(&tls.Config{ServerName: c.Host}); err != nil {
			return err
		}
	}
	if c.Username != "" {
		if err := client.Auth(smtp.PlainAuth("", c.Username, c.Password, c.Host)); err != nil {
			return &PermanentError{fmt.Errorf("smtp auth: %w", err)}
		}
	}
	if err := client.Mail(c.From); err != nil {
		return err
	}
	for _, to := range e.cfg.To {
		if err := client.Rcpt(to); err != nil {
			return err
		}
	}
	wc, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := wc.Write(EmailBytes(c.From, e.cfg.To, m)); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return client.Quit()
}

// EmailBytes renders the RFC 5322 message.
func EmailBytes(from string, to []string, m Message) []byte {
	var b strings.Builder
	header := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	header("From", from)
	header("To", strings.Join(to, ", "))
	header("Subject", mime.QEncoding.Encode("utf-8", render("subject", m)))
	header("Date", m.At.UTC().Format(time.RFC1123Z))
	// Stable per event and recipient list, so a retried send threads with
	// (and can be deduplicated against) the first attempt.
	id := sha256.Sum256([]byte(m.DeliveryID + "|" + strings.Join(to, ",")))
	header("Message-ID", "<"+hex.EncodeToString(id[:12])+"@vigil.invalid>")
	header("MIME-Version", "1.0")
	header("Content-Type", "text/plain; charset=utf-8")
	header("Content-Transfer-Encoding", "8bit")
	header("X-Vigil-Delivery", m.DeliveryID)
	b.WriteString("\r\n")
	body := render("email", m)
	b.WriteString(strings.ReplaceAll(body, "\n", "\r\n"))
	b.WriteString("\r\n")
	return []byte(b.String())
}
