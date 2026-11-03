package web

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type atomFeed struct {
	XMLName xml.Name    `xml:"http://www.w3.org/2005/Atom feed"`
	Title   string      `xml:"title"`
	ID      string      `xml:"id"`
	Updated string      `xml:"updated"`
	Links   []atomLink  `xml:"link"`
	Author  atomPerson  `xml:"author"`
	Entries []atomEntry `xml:"entry"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr,omitempty"`
	Type string `xml:"type,attr,omitempty"`
}

type atomPerson struct {
	Name string `xml:"name"`
}

type atomEntry struct {
	Title   string   `xml:"title"`
	ID      string   `xml:"id"`
	Updated string   `xml:"updated"`
	Link    atomLink `xml:"link"`
	Content atomText `xml:"content"`
}

type atomText struct {
	Type string `xml:"type,attr"`
	Body string `xml:",chardata"`
}

// feed serves an Atom feed of the page's incidents from the last 90 days.
// Each entry's updated time is its latest timeline entry, so feed readers
// resurface an incident when it gets a new update.
func (h *Handler) feed(w http.ResponseWriter, r *http.Request) {
	sp, ok := h.statusPageConfig(w, r)
	if !ok {
		return
	}
	now := h.opts.Clock.Now()
	incidents, err := h.opts.Store.IncidentsBetween(r.Context(), pageMonitors(sp), now.AddDate(0, 0, -90), now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	base := h.baseURL(r)
	pageURL := base + "/status/" + sp.Slug
	names := componentsOf(sp)
	f := atomFeed{
		Title: sp.Title + " incidents",
		ID:    pageURL,
		Links: []atomLink{
			{Href: pageURL + "/feed.atom", Rel: "self", Type: "application/atom+xml"},
			{Href: pageURL, Rel: "alternate", Type: "text/html"},
		},
		Author:  atomPerson{Name: sp.Title},
		Updated: now.UTC().Format(time.RFC3339),
	}
	latest := time.Time{}
	for _, inc := range incidents {
		iv := publicIncident(inc, names[inc.MonitorID], now)
		updated := inc.OpenedAt
		var body strings.Builder
		for _, u := range iv.Updates {
			if u.At.After(updated) {
				updated = u.At
			}
			fmt.Fprintf(&body, "%s (%s): %s\n\n", statusTag(u.Status), u.At.UTC().Format("Jan 2, 15:04 UTC"), u.Body)
		}
		if updated.After(latest) {
			latest = updated
		}
		title := iv.Title
		if !iv.Open {
			title += " (resolved)"
		}
		f.Entries = append(f.Entries, atomEntry{
			Title:   title,
			ID:      fmt.Sprintf("%s/incidents/%d", pageURL, inc.ID),
			Updated: updated.UTC().Format(time.RFC3339),
			Link:    atomLink{Href: fmt.Sprintf("%s/incidents/%d", pageURL, inc.ID), Rel: "alternate", Type: "text/html"},
			Content: atomText{Type: "text", Body: strings.TrimSpace(body.String())},
		})
	}
	if !latest.IsZero() {
		f.Updated = latest.UTC().Format(time.RFC3339)
	}
	w.Header().Set("Content-Type", "application/atom+xml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(f)
}

// baseURL is the configured public URL, or one derived from the request.
func (h *Handler) baseURL(r *http.Request) string {
	if h.opts.BaseURL != "" {
		return strings.TrimSuffix(h.opts.BaseURL, "/")
	}
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}
