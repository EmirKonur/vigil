package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"vigil/internal/alert"
	"vigil/internal/config"
	"vigil/internal/consensus"
	"vigil/internal/probeapi"
	"vigil/internal/store"
)

// DedupKey names one alert for one incident. The outbox has a unique index
// on (notifier, key), so whatever path raises an alert twice (a replayed
// result, a crash between commit and send, two ticks racing a result) only
// one row, and so one delivery, exists per channel.
func DedupKey(monitorID string, opened time.Time, a consensus.Alert, seq int) string {
	key := fmt.Sprintf("incident/%s/%d/%s", monitorID, opened.UnixMilli(), a)
	if seq > 0 {
		key += fmt.Sprintf("/%d", seq)
	}
	return key
}

// applyEffects persists the consequences of a transition inside tx. It
// returns the topics that changed and whether any notification was queued.
func (e *Engine) applyEffects(ctx context.Context, tx *store.Tx, m config.Monitor, st *consensus.State, effects []consensus.Effect) (topics []string, notified bool, err error) {
	if len(effects) > 0 {
		topics = append(topics, TopicIncidents)
	}
	for _, ef := range effects {
		switch ef.Kind {
		case consensus.EffectOpen:
			id, err := tx.OpenIncident(ctx, store.Incident{
				MonitorID: m.ID, OpenedAt: ef.Incident, Impact: string(ef.Impact),
				Regions: ef.Regions, Voting: ef.Voting, Cause: lastError(st, ef.Regions),
			})
			if err != nil {
				return nil, false, err
			}
			body := fmt.Sprintf("Checks are failing from %s: %s.", regionCount(ef.Regions, ef.Voting), strings.Join(ef.Regions, ", "))
			if cause := lastError(st, ef.Regions); cause != "" {
				body += " Last error: " + cause + "."
			}
			if _, err := tx.AddUpdate(ctx, store.Update{IncidentID: id, At: ef.At, Status: "investigating", Body: body, Author: store.AuthorVigil}); err != nil {
				return nil, false, err
			}

		case consensus.EffectImpactChanged:
			id, err := tx.IncidentID(ctx, m.ID, ef.Incident)
			if err != nil {
				return nil, false, err
			}
			widened, err := tx.WidenImpact(ctx, id, string(ef.Impact), ef.Regions, ef.Voting)
			if err != nil {
				return nil, false, err
			}
			// Results inside one slot arrive one region at a time, so an
			// incident often widens milliseconds after opening. Only later
			// widening is news worth a timeline entry; narrowing is the
			// recovery, which the resolution entry covers.
			if widened && ef.At.Sub(ef.Incident) >= m.Interval/2 {
				body := fmt.Sprintf("Now failing from %s: %s.", regionCount(ef.Regions, ef.Voting), strings.Join(ef.Regions, ", "))
				if _, err := tx.AddUpdate(ctx, store.Update{IncidentID: id, At: ef.At, Status: "update", Body: body, Author: store.AuthorVigil}); err != nil {
					return nil, false, err
				}
			}

		case consensus.EffectResolve:
			id, err := tx.IncidentID(ctx, m.ID, ef.Incident)
			if err != nil {
				return nil, false, err
			}
			if err := tx.ResolveIncident(ctx, id, ef.At); err != nil {
				return nil, false, err
			}
			body := fmt.Sprintf("Checks have passed %d times in a row from enough regions. Resolved automatically after %s.",
				m.Policy.RecoverAfter, alert.HumanDuration(ef.At.Sub(ef.Incident)))
			if _, err := tx.AddUpdate(ctx, store.Update{IncidentID: id, At: ef.At, Status: "resolved", Body: body, Author: store.AuthorVigil}); err != nil {
				return nil, false, err
			}

		case consensus.EffectNotify:
			added, err := e.enqueueAlert(ctx, tx, m, st, ef)
			if err != nil {
				return nil, false, err
			}
			notified = notified || added
		}
	}
	return topics, notified, nil
}

func (e *Engine) enqueueAlert(ctx context.Context, tx *store.Tx, m config.Monitor, st *consensus.State, ef consensus.Effect) (bool, error) {
	id, err := tx.IncidentID(ctx, m.ID, ef.Incident)
	if err != nil {
		return false, err
	}
	if ef.Alert == consensus.AlertFlapping {
		if err := tx.SetFlapping(ctx, id); err != nil {
			return false, err
		}
		body := fmt.Sprintf("The check result is flapping. Vigil is keeping this incident open and holding alerts until results are stable for %s.",
			alert.HumanDuration(m.Policy.FlapWindow))
		if _, err := tx.AddUpdate(ctx, store.Update{IncidentID: id, At: ef.At, Status: "update", Body: body, Author: store.AuthorVigil}); err != nil {
			return false, err
		}
	}

	notifiers := m.Notify
	if ef.Alert == consensus.AlertEscalated {
		notifiers = m.Escalate
	} else if ef.Escalated {
		notifiers = append(slices.Clone(m.Notify), m.Escalate...)
	}
	slices.Sort(notifiers)
	notifiers = slices.Compact(notifiers)

	key := DedupKey(m.ID, ef.Incident, ef.Alert, ef.Seq)
	msg := alert.Message{
		Kind:       alert.Kind(ef.Alert),
		DeliveryID: key,
		At:         ef.At,
		Monitor:    alert.Monitor{ID: m.ID, Name: m.Name, Target: m.Spec.Target()},
		Incident: &alert.Incident{
			ID: id, OpenedAt: ef.Incident, Duration: ef.At.Sub(ef.Incident),
			Impact: incidentImpact(st, ef), Regions: ef.Regions, Voting: ef.Voting,
		},
		Reason:     lastError(st, ef.Regions),
		URL:        e.link("/admin/monitors/" + m.ID),
		FlapWindow: m.Policy.FlapWindow,
		Demo:       e.opts.Demo,
	}
	if ef.Alert == consensus.AlertResolved {
		msg.Incident.ResolvedAt = ef.At
	}
	sendAt := ef.At
	if ef.Alert == consensus.AlertOpened || ef.Alert == consensus.AlertFlapping {
		// The other regions' results for this slot are usually a moment
		// behind; waiting one check timeout lets the alert report the
		// incident's full extent rather than the first two regions.
		sendAt = ef.At.Add(m.Spec.Timeout + time.Second)
	}
	return e.enqueue(ctx, tx, notifiers, id, m.ID, key, msg, sendAt)
}

// enqueueCertWarning raises one alert per certificate (identified by its
// expiry) per notifier, however many checks see the warning.
func (e *Engine) enqueueCertWarning(ctx context.Context, tx *store.Tx, m config.Monitor, r probeapi.Result, now time.Time) (bool, error) {
	key := fmt.Sprintf("cert/%s/%d", m.ID, r.CertNotAfter.Unix())
	msg := alert.Message{
		Kind: alert.KindCertExpiring, DeliveryID: key, At: now,
		Monitor: alert.Monitor{ID: m.ID, Name: m.Name, Target: m.Spec.Target()},
		Warning: strings.TrimPrefix(r.Warning, "certificate "),
		URL:     e.link("/admin/monitors/" + m.ID),
		Demo:    e.opts.Demo,
	}
	return e.enqueue(ctx, tx, m.Notify, 0, m.ID, key, msg, now)
}

func (e *Engine) enqueue(ctx context.Context, tx *store.Tx, notifiers []string, incidentID int64, monitorID, key string, msg alert.Message, sendAt time.Time) (bool, error) {
	raw, err := json.Marshal(msg)
	if err != nil {
		return false, err
	}
	queued := false
	for _, n := range notifiers {
		added, err := tx.EnqueueNotification(ctx, store.Notification{
			Notifier: n, DedupKey: key, IncidentID: incidentID, MonitorID: monitorID,
			Kind: string(msg.Kind), Message: raw, CreatedAt: msg.At, NextAttemptAt: sendAt,
		})
		if err != nil {
			return false, err
		}
		queued = queued || added
	}
	return queued, nil
}

func (e *Engine) link(path string) string {
	if e.opts.BaseURL == "" {
		return ""
	}
	return strings.TrimSuffix(e.opts.BaseURL, "/") + path
}

func incidentImpact(st *consensus.State, ef consensus.Effect) string {
	if st.Incident != nil {
		return string(st.Incident.Impact)
	}
	if len(ef.Regions) >= ef.Voting {
		return string(consensus.ImpactMajor)
	}
	return string(consensus.ImpactPartial)
}

// lastError picks a representative error from the failing regions.
func lastError(st *consensus.State, regions []string) string {
	for _, r := range regions {
		if rs, ok := st.Regions[r]; ok && rs.LastError != "" {
			return rs.LastError
		}
	}
	return ""
}

func regionCount(failing []string, voting int) string {
	if len(failing) == 1 && voting == 1 {
		return "the only reporting region"
	}
	return fmt.Sprintf("%d of %d regions", len(failing), voting)
}
