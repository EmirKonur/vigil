// Package alert renders and delivers notifications: generic, Slack, and
// Discord webhooks, and SMTP email.
package alert

import (
	"bytes"
	"embed"
	"fmt"
	"strings"
	"text/template"
	"time"
)

// Kind names what happened.
type Kind string

// Alert kinds. The first five mirror consensus.Alert.
const (
	KindOpened       Kind = "opened"
	KindReminder     Kind = "reminder"
	KindEscalated    Kind = "escalated"
	KindFlapping     Kind = "flapping"
	KindResolved     Kind = "resolved"
	KindCertExpiring Kind = "cert_expiring"
)

// Schema identifies the generic webhook payload version.
const Schema = "vigil.alert.v1"

// Message is everything a notifier needs. It is snapshotted into the outbox
// when the alert is raised, so a retry hours later says what was true then.
type Message struct {
	Schema     string    `json:"schema"`
	Kind       Kind      `json:"kind"`
	DeliveryID string    `json:"delivery_id"`
	At         time.Time `json:"at"`
	Monitor    Monitor   `json:"monitor"`
	Incident   *Incident `json:"incident,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Warning    string    `json:"warning,omitempty"`
	URL        string    `json:"url,omitempty"`
	// FlapWindow is how long the monitor must be stable before a flapping
	// incident resolves.
	FlapWindow time.Duration `json:"flap_window,omitempty"`
	Demo       bool          `json:"demo,omitempty"`
}

// Monitor identifies the monitor an alert is about.
type Monitor struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Target string `json:"target"`
}

// Incident summarizes the incident an alert is about.
type Incident struct {
	ID         int64         `json:"id"`
	OpenedAt   time.Time     `json:"opened_at"`
	ResolvedAt time.Time     `json:"resolved_at,omitzero"`
	Duration   time.Duration `json:"duration"`
	Impact     string        `json:"impact"`
	Regions    []string      `json:"regions"`
	Voting     int           `json:"voting"`
}

//go:embed templates/*.tmpl
var templateFS embed.FS

var templates = template.Must(template.New("alert").Funcs(template.FuncMap{
	"duration": HumanDuration,
	"clock":    func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04 UTC") },
	"join":     strings.Join,
}).ParseFS(templateFS, "templates/*.tmpl"))

func render(name string, m Message) string {
	var b bytes.Buffer
	if err := templates.ExecuteTemplate(&b, name, m); err != nil {
		return fmt.Sprintf("%s: %s (template error: %v)", m.Kind, m.Monitor.Name, err)
	}
	return strings.TrimSpace(b.String())
}

// Title renders the one-line summary.
func Title(m Message) string { return render("title", m) }

// Body renders the short multi-line description.
func Body(m Message) string { return render("body", m) }

// HumanDuration formats a duration the way people say it: "4m", "1h 12m",
// "3d 2h".
func HumanDuration(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		h, m := int(d.Hours()), int(d.Minutes())%60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	default:
		days, h := int(d.Hours())/24, int(d.Hours())%24
		if h == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, h)
	}
}
