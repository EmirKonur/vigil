package engine

import (
	"github.com/prometheus/client_golang/prometheus"

	"vigil/internal/lease"
	"vigil/internal/scheduler"
)

// Metrics are the engine's Prometheus collectors.
type Metrics struct {
	reg           prometheus.Registerer
	Checks        *prometheus.CounterVec
	Latency       *prometheus.HistogramVec
	Results       *prometheus.CounterVec
	MonitorDown   *prometheus.GaugeVec
	Notifications *prometheus.CounterVec
	ProbeLastSeen *prometheus.GaugeVec
}

// NewMetrics registers the engine's collectors with reg. A nil reg keeps
// them unregistered, which tests use.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		reg: reg,
		Checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vigil_check_results_total", Help: "Check results applied, by monitor, region, and outcome (up or down).",
		}, []string{"monitor", "region", "outcome"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "vigil_check_latency_seconds", Help: "Latency of successful checks, by region.",
			Buckets: []float64{.01, .025, .05, .1, .2, .3, .5, .75, 1, 2, 5, 10},
		}, []string{"region"}),
		Results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vigil_probe_reports_total", Help: "Reported results by acknowledgement status (accepted, duplicate, unknown).",
		}, []string{"status"}),
		MonitorDown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vigil_monitor_down", Help: "1 while the monitor has an open incident.",
		}, []string{"monitor"}),
		Notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vigil_notifications_total", Help: "Notification delivery attempts by notifier, kind, and outcome.",
		}, []string{"notifier", "kind", "outcome"}),
		ProbeLastSeen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vigil_probe_last_seen_timestamp_seconds", Help: "Unix time of each probe's last heartbeat.",
		}, []string{"probe", "region"}),
	}
	if reg != nil {
		reg.MustRegister(m.Checks, m.Latency, m.Results, m.MonitorDown, m.Notifications, m.ProbeLastSeen)
	}
	return m
}

func (m *Metrics) observeQueue(q *lease.Queue, s *scheduler.Scheduler) {
	if m.reg == nil {
		return
	}
	counter := func(name, help string, f func(lease.Stats) uint64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 { return float64(f(q.Stats())) })
	}
	gauge := func(name, help string, f func(lease.Stats) int) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return float64(f(q.Stats())) })
	}
	m.reg.MustRegister(
		counter("vigil_jobs_enqueued_total", "Jobs created by the scheduler.", func(s lease.Stats) uint64 { return s.Enqueued }),
		counter("vigil_jobs_superseded_total", "Pending jobs replaced by a newer slot before any probe took them.", func(s lease.Stats) uint64 { return s.Superseded }),
		counter("vigil_jobs_coalesced_total", "Slots skipped because the previous check was still in flight.", func(s lease.Stats) uint64 { return s.Coalesced }),
		counter("vigil_jobs_leased_total", "Jobs handed to probes.", func(s lease.Stats) uint64 { return s.Leased }),
		counter("vigil_leases_requeued_total", "Expired leases given to another probe.", func(s lease.Stats) uint64 { return s.Requeued }),
		counter("vigil_leases_expired_total", "Expired leases that were dropped.", func(s lease.Stats) uint64 { return s.Expired }),
		gauge("vigil_jobs_pending", "Jobs waiting for a probe.", func(s lease.Stats) int { return s.Pending }),
		gauge("vigil_jobs_in_flight", "Jobs currently leased.", func(s lease.Stats) int { return s.InFlight }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "vigil_scheduler_skipped_slots_total", Help: "Slots dropped because the scheduler fell more than an interval behind.",
		}, func() float64 { return float64(s.Skipped()) }),
	)
}
