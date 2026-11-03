package engine

import (
	"context"
	"time"

	"vigil/internal/store"
)

const (
	rollupWatermarkKey = "rollup.hourly.watermark"
	pruneKey           = "prune.last"
)

// rollup folds raw results into hourly and daily rollups, then prunes data
// past its retention. Rollups run from a persisted watermark rather than a
// fixed lookback, so a server that was down for a day catches up instead of
// leaving a hole that pruning later makes permanent.
func (e *Engine) rollup(ctx context.Context) {
	now := e.clock.Now()
	from := now.Add(-48 * time.Hour)
	if v, ok, err := e.store.Meta(ctx, rollupWatermarkKey); err == nil && ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if _, err := e.store.RollupHours(ctx, from, now); err != nil {
		e.logErr(ctx, "rolling up hours", err)
		return
	}
	if _, err := e.store.RollupDays(ctx, from, now); err != nil {
		e.logErr(ctx, "rolling up days", err)
		return
	}
	// Results arrive a few seconds after their slot. Leave the watermark a
	// little behind so the hour that just ended is recomputed once more.
	watermark := now.Add(-5 * time.Minute).UTC().Truncate(time.Hour)
	if err := e.store.SetMeta(ctx, rollupWatermarkKey, watermark.Format(time.RFC3339)); err != nil {
		e.logErr(ctx, "saving rollup watermark", err)
	}

	if v, ok, _ := e.store.Meta(ctx, pruneKey); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil && now.Sub(t) < time.Hour {
			return
		}
	}
	cfg := e.Config()
	if cfg == nil {
		return
	}
	n, err := e.store.Prune(ctx, now, store.Retention{
		Raw: time.Duration(cfg.Retention.Raw), Hourly: time.Duration(cfg.Retention.Hourly), Daily: time.Duration(cfg.Retention.Daily),
	})
	if err != nil {
		e.logErr(ctx, "pruning", err)
		return
	}
	if n > 0 {
		e.log.Info("pruned expired data", "rows", n)
	}
	_ = e.store.SetMeta(ctx, pruneKey, now.UTC().Format(time.RFC3339))
}

func (e *Engine) logErr(ctx context.Context, msg string, err error) {
	if ctx.Err() == nil {
		e.log.Error(msg, "err", err)
	}
}
