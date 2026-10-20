package gateway

import (
	"context"

	"errors"

	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
)

// AvailabilitySnapshot returns the availability state of every free model in
// the current runtime's catalog.
func (m *RuntimeController) AvailabilitySnapshot() []models.Availability {
	r := m.current.Load()
	if r == nil {
		return []models.Availability{}
	}
	return availabilityOf(r)
}

// FreeModelStatus is one free model's probe state plus the context window its
// anonymous route advertises. The anonymous lane serves free models from the
// Zen tier, so the window comes from that tier's metadata, with the pricing
// metadata filling in when the capability catalog omitted the limit; it is 0
// when neither source states one.
type FreeModelStatus struct {
	models.Availability
	ContextWindow int `json:"context_window,omitempty"`
}

// AvailabilityReport returns the availability rows of the current runtime with
// the advertised context window attached, for the operator-facing free model
// page.
func (m *RuntimeController) AvailabilityReport() []FreeModelStatus {
	r := m.current.Load()
	if r == nil {
		return []FreeModelStatus{}
	}
	rows := availabilityOf(r)
	items := make([]FreeModelStatus, 0, len(rows))
	for _, item := range rows {
		items = append(items, FreeModelStatus{
			Availability:  item,
			ContextWindow: r.gateway.catalog.MetadataForTier(item.Model, config.TierZen).ContextWindow,
		})
	}
	return items
}

// availabilityOf lists the probe state of every free model in r's catalog. It
// takes the runtime explicitly so a caller cannot pair rows of one runtime with
// metadata of another.
func availabilityOf(r *gatewayRuntime) []models.Availability {
	items := []models.Availability{}
	for _, model := range r.gateway.catalog.List() {
		if r.gateway.catalog.IsFreeModel(model) {
			items = append(items, r.availability.Get(model))
		}
	}
	return items
}

// RestoreModel re-enables a free model by putting it under manual control
// (see AvailabilityRegistry.Restore), which also discards the results of
// probes already in flight. It fails for a model that is not a free model of
// the current catalog.
func (m *RuntimeController) RestoreModel(model string) error {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	r := m.current.Load()
	if r == nil {
		return errors.New("gateway runtime is unavailable")
	}
	for _, item := range m.AvailabilitySnapshot() {
		if item.Model == model {
			return r.availability.Restore(model, time.Now().UTC())
		}
	}
	return errors.New("unknown free model")
}

// SetManualModel switches a free model between manual and automatic control.
// Manual models stay enabled and are excluded from automatic probing.
func (m *RuntimeController) SetManualModel(model string, manual bool) error {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	r := m.current.Load()
	if r == nil {
		return errors.New("gateway runtime is unavailable")
	}
	for _, item := range m.AvailabilitySnapshot() {
		if item.Model == model {
			return r.availability.SetManual(model, manual, time.Now().UTC())
		}
	}
	return errors.New("unknown free model")
}

// startAvailabilityChecks probes due free models at once and then every
// minute until Shutdown or the root context stops it. It runs on the
// controller's root context, so it survives runtime swaps.
func (m *RuntimeController) startAvailabilityChecks() {
	ctx, cancel := context.WithCancel(m.root)
	m.availabilityCancel = cancel
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			m.checkFreeModels(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// checkFreeModels probes, one at a time, every free model under automatic
// control whose next check is due. A result is recorded only while the
// runtime that ran the probe is still current.
func (m *RuntimeController) checkFreeModels(ctx context.Context) {
	for _, item := range m.AvailabilitySnapshot() {
		if ctx.Err() != nil {
			return
		}
		r := m.current.Load()
		if r == nil {
			return
		}
		item = r.availability.Get(item.Model)
		if item.Manual {
			continue
		}
		if time.Now().Before(item.NextCheck) {
			continue
		}
		result := r.gateway.probeFreeModel(ctx, item.Model)
		if !result.attempted || ctx.Err() != nil {
			continue
		}
		// Config changes may replace credentials or endpoints during a probe.
		m.updateMu.Lock()
		if m.current.Load() == r {
			if err := r.availability.Record(item.Model, item.Generation, result.success, result.reason, result.channel, time.Now().UTC(), result.effort); err != nil {
				m.logger.Warn("model availability state save failed", "error", err)
			}
			m.logger.Info("free model availability checked", "component", "models", "event", "availability_checked", "model", item.Model, "available", result.success, "reason", result.reason, "channel", result.channel)
		}
		m.updateMu.Unlock()
	}
}
