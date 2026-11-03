// Package probeapi defines the messages exchanged between the server and
// probes. The same types travel over HTTP for remote probes and as plain
// function arguments for the probe embedded in the server.
package probeapi

import (
	"context"
	"time"

	"vigil/internal/check"
	"vigil/internal/lease"
)

// Paths of the HTTP endpoints.
const (
	LeasePath  = "/api/v1/probe/lease"
	ReportPath = "/api/v1/probe/results"
)

// LeaseRequest asks for up to Max jobs, waiting up to Wait for one.
type LeaseRequest struct {
	ProbeID string        `json:"probe_id"`
	Region  string        `json:"region"`
	Version string        `json:"version"`
	Max     int           `json:"max"`
	Wait    time.Duration `json:"wait"`
	// ProbeTime is the probe's wall clock when it sent the request. The
	// server only uses it to display clock skew; leases are relative
	// durations precisely so that skew cannot break them.
	ProbeTime time.Time `json:"probe_time"`
}

// LeaseResponse carries leased jobs.
type LeaseResponse struct {
	Jobs       []lease.Leased `json:"jobs"`
	ServerTime time.Time      `json:"server_time"`
}

// Result is one completed job.
type Result struct {
	JobID string `json:"job_id"`
	check.Result
}

// ReportRequest delivers results. Probes retry until acknowledged, so the
// server must treat a repeated JobID as already handled.
type ReportRequest struct {
	ProbeID string   `json:"probe_id"`
	Region  string   `json:"region"`
	Results []Result `json:"results"`
}

// Ack statuses.
const (
	AckAccepted  = "accepted"  // stored and applied
	AckDuplicate = "duplicate" // already stored; a retry or a second probe's late answer
	AckUnknown   = "unknown"   // never issued, or issued by a server process that has since restarted
)

// Ack tells the probe it may forget a result.
type Ack struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// ReportResponse acknowledges each result.
type ReportResponse struct {
	Acks []Ack `json:"acks"`
}

// API is what a probe needs from the server.
type API interface {
	Lease(ctx context.Context, req LeaseRequest) (LeaseResponse, error)
	Report(ctx context.Context, req ReportRequest) (ReportResponse, error)
}
