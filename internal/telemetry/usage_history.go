package telemetry

// Local: usage history (hour buckets + disk persistence).
// This file owns usageHourBucket/SaveUsageHistory/LoadUsageHistory.
// metrics.go keeps the minute-bucket Monitor skeleton; usage_range.go owns
// Beijing alignment/aggregation/TopN; usage_cost.go owns price helpers.

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// Long-term token-usage history. Minute buckets stay in memory for the last
// hour; hourly buckets persist to disk (config.json sibling) and roll up to
// Beijing day/week/month on query. Costs are captured at record time with the
// current models.dev price (USD per 1M tokens) so later price refreshes do
// not rewrite history; unknown prices record zero cost but keep tokens.
const (
	usageHistoryVersion       = 1
	usageHistoryRetentionDays = 400
	usageHistoryMaxModels     = 500
)

type usageHourBucket struct {
	Hour       int64                  `json:"hour"`
	Requests   uint64                 `json:"requests"`
	Reported   uint64                 `json:"reported"`
	Tokens     TokenCounts            `json:"tokens"`
	Models     map[string]TokenCounts `json:"models"`
	Tiers      map[string]TokenCounts `json:"tiers"`
	CostUSD    float64                `json:"cost_usd"`
	ModelCosts map[string]float64     `json:"model_costs"`
	TierCosts  map[string]float64     `json:"tier_costs"`
	// UnknownCost marks buckets containing requests whose model price was
	// unknown at record time: their cost reads $0 exactly like a known-free
	// model, and only this flag tells the two apart.
	UnknownCost    bool                   `json:"unknown_cost,omitempty"`
	Endpoints      map[string]uint64      `json:"endpoints,omitempty"`
	EndpointTokens map[string]TokenCounts `json:"endpoint_tokens,omitempty"`
	EndpointCosts  map[string]float64     `json:"endpoint_costs,omitempty"`
}

type usageHistoryFile struct {
	Version int               `json:"version"`
	Hours   []usageHourBucket `json:"hours"`
	// RecentRequests carries the per-request detail ring (last 24h,
	// newest last) so the request table survives restarts. Absent in
	// files written before this field existed. Entries hold no secrets
	// (key_id is tail-only, no client IP/keys are recorded) but may carry
	// the session (the canonical `ses_` id the request went out with,
	// used only to correlate one conversation's requests).
	RecentRequests []UpstreamRequest `json:"recent_requests,omitempty"`
}

// newUsageHourBucket allocates an hour bucket with initialized sub-maps.
func newUsageHourBucket(hour int64) *usageHourBucket {
	return &usageHourBucket{
		Hour: hour, Models: make(map[string]TokenCounts), Tiers: make(map[string]TokenCounts),
		ModelCosts: make(map[string]float64), TierCosts: make(map[string]float64),
		Endpoints: make(map[string]uint64), EndpointTokens: make(map[string]TokenCounts),
		EndpointCosts: make(map[string]float64),
	}
}

// pruneUsageHoursLocked drops buckets older than the retention window,
// reporting whether anything was removed. The caller must hold m.mu.
func (m *Monitor) pruneUsageHoursLocked(now time.Time) bool {
	cutoff := now.Add(-time.Duration(usageHistoryRetentionDays) * 24 * time.Hour).Truncate(time.Hour).Unix()
	removed := false
	for hour := range m.usageHours {
		if hour < cutoff {
			delete(m.usageHours, hour)
			removed = true
		}
	}
	return removed
}

// UsageHistoryPath returns the sibling path for the usage history file,
// derived the same way as the other runtime files (rotation, models catalog).
func UsageHistoryPath(configPath string) string {
	if configPath == "" {
		return ""
	}
	return configPath + ".usage.history.json"
}

func (m *Monitor) snapshotUsageHoursLocked() []usageHourBucket {
	result := make([]usageHourBucket, 0, len(m.usageHours))
	for _, bucket := range m.usageHours {
		clone := usageHourBucket{
			Hour: bucket.Hour, Requests: bucket.Requests, Reported: bucket.Reported,
			Tokens: bucket.Tokens, CostUSD: bucket.CostUSD, UnknownCost: bucket.UnknownCost,
			Models:         make(map[string]TokenCounts, len(bucket.Models)),
			Tiers:          make(map[string]TokenCounts, len(bucket.Tiers)),
			ModelCosts:     make(map[string]float64, len(bucket.ModelCosts)),
			TierCosts:      make(map[string]float64, len(bucket.TierCosts)),
			Endpoints:      make(map[string]uint64, len(bucket.Endpoints)),
			EndpointTokens: make(map[string]TokenCounts, len(bucket.EndpointTokens)),
			EndpointCosts:  make(map[string]float64, len(bucket.EndpointCosts)),
		}
		for key, value := range bucket.Models {
			clone.Models[key] = value
		}
		for key, value := range bucket.Tiers {
			clone.Tiers[key] = value
		}
		for key, value := range bucket.ModelCosts {
			clone.ModelCosts[key] = value
		}
		for key, value := range bucket.TierCosts {
			clone.TierCosts[key] = value
		}
		for key, value := range bucket.Endpoints {
			clone.Endpoints[key] = value
		}
		for key, value := range bucket.EndpointTokens {
			clone.EndpointTokens[key] = value
		}
		for key, value := range bucket.EndpointCosts {
			clone.EndpointCosts[key] = value
		}
		result = append(result, clone)
	}
	// No sort here: the caller sorts after releasing m.mu (sorting a local
	// slice under the hot Record lock is pure contention).
	return result
}

// sortUsageHours orders a snapshot for stable on-disk output.
func sortUsageHours(hours []usageHourBucket) {
	sort.Slice(hours, func(i, j int) bool { return hours[i].Hour < hours[j].Hour })
}

// SaveUsageHistory atomically persists hourly buckets beside config.json.
// The minute ticker calls this unconditionally; when nothing changed since
// the last save only the cheap retention prune runs and the snapshot,
// marshal and disk write are skipped. After a failed Load the save is
// refused outright so an empty monitor never overwrites on-disk history.
func (m *Monitor) SaveUsageHistory(path string) error {
	if path == "" {
		return nil
	}
	m.mu.Lock()
	now := time.Now().UTC()
	if m.pruneUsageHoursLocked(now) {
		// Expiry removed buckets: the on-disk image is stale even with no
		// new traffic, so force this tick through the write path.
		m.usageDirty.Store(true)
	}
	if m.loadFailed.Load() {
		m.mu.Unlock()
		return fmt.Errorf("refusing to save usage history after a failed load (on-disk data preserved)")
	}
	if !m.usageDirty.Swap(false) && m.historySaved.Load() {
		m.mu.Unlock()
		return nil
	}
	hours := m.snapshotUsageHoursLocked()
	m.recentRequests.PruneBefore(now.Add(-upstreamRequestRetention))
	// Persist exactly the window the API/UI can show (monitorRecentOutputLimit
	// newest within upstreamRequestRetention); older entries would occupy
	// disk without ever being visible.
	recent := m.recentRequests.Snapshot(now.Add(-upstreamRequestRetention), monitorRecentOutputLimit)
	m.mu.Unlock()
	sortUsageHours(hours)
	// Compact, not indented: the whole file is re-marshaled on every save, and
	// at the 400-day/hour-bucket ceiling (~9.6k buckets) indentation alone adds
	// ~40% to the bytes written. Nothing reads this file by hand (Load parses
	// JSON; `jq`/`ConvertFrom-Json` do not care), so prettiness is not worth
	// the write amplification.
	data, err := json.Marshal(usageHistoryFile{Version: usageHistoryVersion, Hours: hours, RecentRequests: recent})
	if err != nil {
		// The snapshot was already consumed (dirty cleared above): re-arm
		// so the records are not silently dropped until the next Record.
		m.usageDirty.Store(true)
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".usage-history-*.tmp")
	if err != nil {
		m.usageDirty.Store(true)
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err = temp.Chmod(0600); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		m.usageDirty.Store(true)
		return err
	}
	if runtime.GOOS == "windows" {
		backup := path + ".replace"
		_ = os.Remove(backup)
		if _, statErr := os.Stat(path); statErr == nil {
			if err := os.Rename(path, backup); err != nil {
				m.usageDirty.Store(true)
				return err
			}
		}
		if err := os.Rename(tempPath, path); err != nil {
			_ = os.Rename(backup, path)
			m.usageDirty.Store(true)
			return err
		}
		_ = os.Remove(backup)
		m.historySaved.Store(true)
		return os.Chmod(path, 0600)
	}
	if err := os.Rename(tempPath, path); err != nil {
		m.usageDirty.Store(true)
		return err
	}
	m.historySaved.Store(true)
	return os.Chmod(path, 0600)
}

// openHistoryFile is os.Open behind a seam so the "unreadable for another
// reason" branch (permissions, transient IO) can be exercised on any
// platform: on Windows chmod cannot make a file unreadable the way it does
// on Unix, so a test-only failure injector is the only portable way in.
var openHistoryFile = os.Open

// LoadUsageHistory restores hourly buckets; missing file is not an error.
// It reports whether an interrupted replace had to be recovered (see
// recoverReplacedHistory) so the caller can log that separately from a real
// load failure. Load is startup-only into an empty monitor: a second Load
// (or one racing live traffic) is refused, and a repeated Hour inside one
// file takes the last entry instead of double-counting.
func (m *Monitor) LoadUsageHistory(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	m.mu.Lock()
	loaded := len(m.usageHours) > 0 || m.recentRequests.count > 0
	m.mu.Unlock()
	if loaded {
		return false, fmt.Errorf("usage history already loaded")
	}
	recovered := false
	file, err := openHistoryFile(path)
	if err != nil && os.IsNotExist(err) {
		// An interrupted Windows replace leaves the whole history under
		// path+".replace" with no main file. Adopt the backup and read it
		// below; returning empty here would let the first save write an empty
		// file over the only copy of the data.
		recovered, err = m.recoverReplacedHistory(path)
		if err != nil {
			return false, err
		}
		if !recovered {
			return false, nil
		}
		file, err = openHistoryFile(path)
	}
	if err != nil {
		// Unreadable for another reason (permissions, transient IO): fence
		// the monitor too, so a tick cannot persist empty state over data
		// we simply failed to read.
		m.loadFailed.Store(true)
		return false, err
	}
	defer file.Close()
	// Pre-check size before decoding: a legitimate history beyond the
	// decode bound must NOT be truncated (truncation reads as
	// "unexpected EOF" and would mislabel good data as corrupt). It is
	// preserved under a distinct .oversized suffix for manual trimming,
	// and the monitor is fenced via loadFailed so the next Save cannot
	// overwrite it with empty state.
	const usageHistoryDecodeBound = 64 << 20
	if stat, statErr := file.Stat(); statErr == nil && stat.Size() > usageHistoryDecodeBound {
		_ = file.Close()
		if preserved, ok := preserveBadHistory(path, "oversized"); ok {
			m.loadFailed.Store(true)
			return false, fmt.Errorf("usage history too large (%d bytes): preserved at %s for manual trimming, refusing to load/overwrite", stat.Size(), preserved)
		}
		m.loadFailed.Store(true)
		return false, fmt.Errorf("usage history too large (%d bytes), refusing to load/overwrite", stat.Size())
	}
	var stored usageHistoryFile
	decoder := json.NewDecoder(io.LimitReader(file, usageHistoryDecodeBound))
	if err := decoder.Decode(&stored); err != nil {
		// Preserve the damaged file for forensics before reporting: the
		// caller only warns on Load failure, and the next minute-tick Save
		// would otherwise overwrite the on-disk history with the still-empty
		// monitor, losing everything permanently. The handle must be closed
		// first: Windows cannot rename an open file (the deferred Close
		// below then only sees an already-closed file, which is harmless).
		// The monitor is fenced even if the rename itself fails.
		m.loadFailed.Store(true)
		_ = file.Close()
		if preserved, ok := preserveBadHistory(path, "corrupt"); ok {
			return false, fmt.Errorf("decode usage history: %w (damaged file preserved at %s)", err, preserved)
		}
		return false, fmt.Errorf("decode usage history: %w", err)
	}
	// Only a newer-than-known version is rejected: older files simply decode
	// with zero values for fields they predate (nil maps are allocated
	// below, a missing request ring decodes as empty), so they stay loadable.
	// A missing version (0) likewise loads as empty history; the file itself
	// is left on disk either way, so nothing is lost.
	if stored.Version > usageHistoryVersion {
		// Rejected, but the on-disk file is fine — fence the empty monitor
		// so a later tick cannot overwrite good history with nothing.
		m.loadFailed.Store(true)
		return false, fmt.Errorf("unsupported usage history version %d", stored.Version)
	}
	now := time.Now().UTC()
	cutoff := now.Add(-time.Duration(usageHistoryRetentionDays) * 24 * time.Hour).Truncate(time.Hour).Unix()
	m.mu.Lock()
	defer m.mu.Unlock()
	// Recheck under the merge lock: Load is startup-only, but anything that
	// filled the monitor between the early check and here must still be
	// refused instead of double-counted.
	if len(m.usageHours) > 0 || m.recentRequests.count > 0 {
		return false, fmt.Errorf("usage history already loaded")
	}
	if m.usageHours == nil {
		m.usageHours = make(map[int64]*usageHourBucket)
	}
	for _, item := range stored.Hours {
		if item.Hour < cutoff {
			continue
		}
		// Last wins: the merge path of the old code is unreachable for a
		// well-formed file (Load is refused on a non-empty monitor), so a
		// repeated Hour here means a duplicated entry inside this one file.
		// Replacing (not accumulating) keeps a single-file duplicate from
		// double-counting.
		clone := item
		clone.Models = cloneMapOrNew(item.Models)
		clone.Tiers = cloneMapOrNew(item.Tiers)
		if item.ModelCosts == nil {
			clone.ModelCosts = make(map[string]float64)
		}
		if item.TierCosts == nil {
			clone.TierCosts = make(map[string]float64)
		}
		if item.Endpoints == nil {
			clone.Endpoints = make(map[string]uint64)
		}
		if item.EndpointTokens == nil {
			clone.EndpointTokens = make(map[string]TokenCounts)
		}
		if item.EndpointCosts == nil {
			clone.EndpointCosts = make(map[string]float64)
		}
		m.usageHours[item.Hour] = &clone
	}
	// Restore the per-request ring in stored (oldest-first) order, keeping
	// only entries still inside the retention window the live ring enforces.
	requestCutoff := now.Add(-upstreamRequestRetention)
	// Load runs once on an empty ring at startup; pruning first keeps a
	// repeated Load from interleaving old entries ahead of live ones and
	// wedging PruneBefore behind them.
	m.recentRequests.PruneBefore(requestCutoff)
	for _, request := range stored.RecentRequests {
		if request.Time.Before(requestCutoff) {
			continue
		}
		m.recentRequests.Add(request)
	}
	m.pruneUsageHoursLocked(now)
	// A successful load re-arms saving (e.g. after an operator restores a
	// preserved .corrupt/.oversized file and restarts into a retry). The
	// already-loaded refusals above intentionally leave the fence alone.
	m.loadFailed.Store(false)
	return recovered, nil
}

// recoverReplacedHistory adopts the backup left behind when the Windows
// replace sequence was interrupted: the process was killed between renaming
// the live file to path+".replace" and renaming the new file into place, so
// the complete history exists only under the backup name. Treating the
// missing main file as "no history yet" would let the first tick write an
// empty file over the top of it. It reports whether a backup was adopted; a
// backup that exists but cannot be moved back fences the monitor instead of
// letting the next save strand it.
func (m *Monitor) recoverReplacedHistory(path string) (bool, error) {
	backup := path + ".replace"
	if info, err := os.Stat(backup); err != nil || info.IsDir() {
		return false, nil
	}
	if err := os.Rename(backup, path); err != nil {
		m.loadFailed.Store(true)
		return false, fmt.Errorf("usage history replace backup %s could not be restored: %w", backup, err)
	}
	return true, nil
}

// cloneMapOrNew copies a token map, allocating an empty one for nil input.
func cloneMapOrNew(source map[string]TokenCounts) map[string]TokenCounts {
	result := make(map[string]TokenCounts, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

// preserveBadHistory renames an unloadable history file aside for forensics
// (suffix "corrupt" or "oversized") and trims old .corrupt keeps to the
// newest few so a repeatedly-failing Load cannot fill the disk.
func preserveBadHistory(path, suffix string) (string, bool) {
	preserved := fmt.Sprintf("%s.%s.%d", path, suffix, time.Now().UnixNano())
	if err := os.Rename(path, preserved); err != nil {
		return "", false
	}
	if suffix == "corrupt" || suffix == "oversized" {
		keeps, _ := filepath.Glob(path + "." + suffix + ".*")
		sort.Strings(keeps)
		for len(keeps) > 5 {
			_ = os.Remove(keeps[0])
			keeps = keeps[1:]
		}
	}
	return preserved, true
}
