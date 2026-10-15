package telemetry

// Local: usage range (Beijing alignment, aggregation, TopN trim).
// This file owns UsagePoint/UsageRangeResult and the range query path.
// metrics.go keeps the Monitor skeleton; usage_history.go owns hour
// buckets/persistence; usage_cost.go owns record-time price helpers.

import (
	"fmt"
	"sort"
	"time"
)

// BeijingZone is the single display/aggregation timezone for usage ranges.
var BeijingZone = time.FixedZone("Asia/Shanghai", 8*3600)

const (
	usageRangeTopN           = 15
	usageRangeMaxHourPoints  = 168
	usageRangeMaxDayPoints   = 400
	usageRangeMaxWeekPoints  = 60
	usageRangeMaxMonthPoints = 48
	// usageRequestDetailLimit caps the per-request detail attached to a
	// range result, matching what the usage page renders.
	usageRequestDetailLimit = 500
)

// UsagePoint is one Beijing-aligned bucket (or the rolled-up total).
type UsagePoint struct {
	Start      time.Time              `json:"start"`
	End        time.Time              `json:"end"`
	Requests   uint64                 `json:"requests"`
	Reported   uint64                 `json:"reported"`
	Coverage   float64                `json:"coverage"`
	Tokens     TokenCounts            `json:"tokens"`
	Models     map[string]TokenCounts `json:"models"`
	Tiers      map[string]TokenCounts `json:"tiers"`
	CostUSD    float64                `json:"cost_usd"`
	ModelCosts map[string]float64     `json:"model_costs"`
	TierCosts  map[string]float64     `json:"tier_costs"`
	// UnknownCost is true when any folded bucket held requests with an
	// unknown model price (see usageHourBucket): $0 may mean "free" or
	// "unpriced", and the UI must not conflate them.
	UnknownCost    bool                   `json:"unknown_cost,omitempty"`
	Endpoints      map[string]uint64      `json:"endpoints"`
	EndpointTokens map[string]TokenCounts `json:"endpoint_tokens"`
	EndpointCosts  map[string]float64     `json:"endpoint_costs"`
}

// UsageRangeResult backs GET /api/usage. Timezone is always Asia/Shanghai.
type UsageRangeResult struct {
	Granularity string       `json:"granularity"`
	Timezone    string       `json:"timezone"`
	From        time.Time    `json:"from"`
	To          time.Time    `json:"to"`
	Points      []UsagePoint `json:"points"`
	Total       UsagePoint   `json:"total"`
	// Requests is the per-request detail inside [From, To), newest first and
	// capped at usageRequestDetailLimit rows. The ring keeps a day, so a
	// longer queried range returns fewer rows rather than more.
	Requests []UpstreamRequest `json:"requests,omitempty"`
	// HistoryDisabled reports that disk persistence is off for this process
	// (see LoadUsageHistory): the numbers above are memory-only and will be
	// lost on restart, so the UI says so instead of looking healthy.
	HistoryDisabled bool `json:"history_disabled,omitempty"`
}

// Beijing-aligned truncation helpers. Weeks start Monday, months are natural.
func beijingHourStart(t time.Time) time.Time {
	bt := t.In(BeijingZone)
	return time.Date(bt.Year(), bt.Month(), bt.Day(), bt.Hour(), 0, 0, 0, BeijingZone)
}

func beijingDayStart(t time.Time) time.Time {
	bt := t.In(BeijingZone)
	return time.Date(bt.Year(), bt.Month(), bt.Day(), 0, 0, 0, 0, BeijingZone)
}

func beijingWeekStart(t time.Time) time.Time {
	bt := t.In(BeijingZone)
	offset := (int(bt.Weekday()) + 6) % 7
	day := time.Date(bt.Year(), bt.Month(), bt.Day(), 0, 0, 0, 0, BeijingZone)
	return day.AddDate(0, 0, -offset)
}

func beijingMonthStart(t time.Time) time.Time {
	bt := t.In(BeijingZone)
	return time.Date(bt.Year(), bt.Month(), 1, 0, 0, 0, 0, BeijingZone)
}

// truncateBeijing snaps t to the start of its granularity bucket.
func truncateBeijing(t time.Time, granularity string) time.Time {
	switch granularity {
	case "hour":
		return beijingHourStart(t)
	case "day":
		return beijingDayStart(t)
	case "week":
		return beijingWeekStart(t)
	case "month":
		return beijingMonthStart(t)
	default:
		return beijingDayStart(t)
	}
}

// addBeijingGranularity advances t by n granularity steps (calendar-aware).
func addBeijingGranularity(t time.Time, granularity string, n int) time.Time {
	switch granularity {
	case "hour":
		return t.Add(time.Duration(n) * time.Hour)
	case "day":
		return t.AddDate(0, 0, n)
	case "week":
		return t.AddDate(0, 0, 7*n)
	case "month":
		return t.AddDate(0, n, 0)
	default:
		return t.AddDate(0, 0, n)
	}
}

// usageRangeLimit caps the number of points per granularity.
func usageRangeLimit(granularity string) int {
	switch granularity {
	case "hour":
		return usageRangeMaxHourPoints
	case "day":
		return usageRangeMaxDayPoints
	case "week":
		return usageRangeMaxWeekPoints
	case "month":
		return usageRangeMaxMonthPoints
	default:
		return 0
	}
}

// newUsagePoint allocates a point with initialized sub-maps.
func newUsagePoint(start, end time.Time) UsagePoint {
	return UsagePoint{
		Start: start.UTC(), End: end.UTC(),
		Models: make(map[string]TokenCounts), Tiers: make(map[string]TokenCounts),
		ModelCosts: make(map[string]float64), TierCosts: make(map[string]float64),
		Endpoints: make(map[string]uint64), EndpointTokens: make(map[string]TokenCounts),
		EndpointCosts: make(map[string]float64),
	}
}

// accumulateUsagePoint folds one hour bucket into a range point.
func accumulateUsagePoint(target *UsagePoint, bucket *usageHourBucket) {
	if target.Models == nil {
		target.Models = make(map[string]TokenCounts)
	}
	if target.Tiers == nil {
		target.Tiers = make(map[string]TokenCounts)
	}
	if target.ModelCosts == nil {
		target.ModelCosts = make(map[string]float64)
	}
	if target.TierCosts == nil {
		target.TierCosts = make(map[string]float64)
	}
	if target.Endpoints == nil {
		target.Endpoints = make(map[string]uint64)
	}
	if target.EndpointTokens == nil {
		target.EndpointTokens = make(map[string]TokenCounts)
	}
	if target.EndpointCosts == nil {
		target.EndpointCosts = make(map[string]float64)
	}
	target.Requests += bucket.Requests
	target.Reported += bucket.Reported
	addTokenCounts(&target.Tokens, bucket.Tokens)
	mergeTokenMaps(target.Models, bucket.Models)
	mergeTokenMaps(target.Tiers, bucket.Tiers)
	mergeCounts(target.Endpoints, bucket.Endpoints)
	mergeTokenMaps(target.EndpointTokens, bucket.EndpointTokens)
	target.CostUSD += bucket.CostUSD
	if bucket.UnknownCost {
		target.UnknownCost = true
	}
	for key, value := range bucket.ModelCosts {
		target.ModelCosts[key] += value
	}
	for key, value := range bucket.TierCosts {
		target.TierCosts[key] += value
	}
	for key, value := range bucket.EndpointCosts {
		target.EndpointCosts[key] += value
	}
}

// finalizeUsagePoint computes coverage and trims sub-maps to TopN.
func finalizeUsagePoint(point *UsagePoint) {
	if point.Requests > 0 {
		point.Coverage = float64(point.Reported) / float64(point.Requests)
	}
	trimUsagePoint(point, usageRangeTopN)
}

// trimUsagePoint keeps the TopN models/tiers and drops costs for evicted keys.
func trimUsagePoint(point *UsagePoint, topN int) {
	point.Models = trimTokenMap(point.Models, topN)
	point.Tiers = trimTokenMap(point.Tiers, topN)
	point.ModelCosts = trimCostMap(point.ModelCosts, point.Models)
	point.TierCosts = trimCostMap(point.TierCosts, point.Tiers)
}

// trimTokenMap keeps the TopN entries by total tokens (ties by key).
func trimTokenMap(source map[string]TokenCounts, topN int) map[string]TokenCounts {
	if len(source) <= topN {
		return source
	}
	type entry struct {
		key   string
		total uint64
	}
	entries := make([]entry, 0, len(source))
	for key, value := range source {
		entries = append(entries, entry{key: key, total: value.Total})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].total == entries[j].total {
			return entries[i].key < entries[j].key
		}
		return entries[i].total > entries[j].total
	})
	result := make(map[string]TokenCounts, topN)
	for _, item := range entries[:topN] {
		result[item.key] = source[item.key]
	}
	return result
}

// trimCostMap drops cost entries whose token key was trimmed away.
func trimCostMap(costs map[string]float64, kept map[string]TokenCounts) map[string]float64 {
	if len(costs) == 0 {
		return costs
	}
	result := make(map[string]float64, len(costs))
	for key, value := range costs {
		if _, ok := kept[key]; ok {
			result[key] = value
		}
	}
	return result
}

// UsageRangeExplicit aggregates hourly buckets into Beijing-aligned points.
func (m *Monitor) UsageRangeExplicit(granularity string, from, to time.Time) (UsageRangeResult, error) {
	switch granularity {
	case "hour", "day", "week", "month":
	default:
		return UsageRangeResult{}, fmt.Errorf("granularity must be hour, day, week, or month")
	}
	if from.IsZero() || to.IsZero() || !from.Before(to) {
		return UsageRangeResult{}, fmt.Errorf("invalid time range")
	}
	from = truncateBeijing(from, granularity)
	to = truncateBeijing(to, granularity)
	if !from.Before(to) {
		to = addBeijingGranularity(from, granularity, 1)
	}
	count := 0
	for cursor := from; cursor.Before(to); cursor = addBeijingGranularity(cursor, granularity, 1) {
		count++
		if count > usageRangeLimit(granularity) {
			return UsageRangeResult{}, fmt.Errorf("range exceeds %d %s points", usageRangeLimit(granularity), granularity)
		}
	}
	points := make([]UsagePoint, 0, count)
	index := make(map[int64]int)
	for cursor := from; cursor.Before(to); cursor = addBeijingGranularity(cursor, granularity, 1) {
		end := addBeijingGranularity(cursor, granularity, 1)
		index[truncateBeijing(cursor, granularity).Unix()] = len(points)
		points = append(points, newUsagePoint(cursor, end))
	}
	total := newUsagePoint(from, to)
	var requests []UpstreamRequest
	// Read-only traversal under the same single mutex Record takes: buckets
	// and the request ring are only read here, and accumulation targets are
	// function-local. The Monitor keeps one lock (no RWMutex split), so this
	// briefly blocks concurrent Record calls instead of running beside them.
	m.mu.Lock()
	for _, bucket := range m.usageHours {
		hourTime := time.Unix(bucket.Hour, 0).UTC()
		if hourTime.Before(from) || !hourTime.Before(to) {
			continue
		}
		key := truncateBeijing(hourTime, granularity).Unix()
		if i, ok := index[key]; ok {
			accumulateUsagePoint(&points[i], bucket)
		}
		accumulateUsagePoint(&total, bucket)
	}
	requests = m.recentRequests.SnapshotWithin(from, to, usageRequestDetailLimit)
	historyDisabled := m.loadFailed.Load()
	m.mu.Unlock()
	for i := range points {
		finalizeUsagePoint(&points[i])
	}
	finalizeUsagePoint(&total)
	return UsageRangeResult{
		Granularity: granularity, Timezone: "Asia/Shanghai",
		From: from.UTC(), To: to.UTC(), Points: points, Total: total,
		Requests: requests, HistoryDisabled: historyDisabled,
	}, nil
}

// UsageRangeCount returns the latest count buckets including the current
// Beijing bucket, shifted back by offset pages for prev/next navigation.
func (m *Monitor) UsageRangeCount(granularity string, count, offset int) (UsageRangeResult, error) {
	switch granularity {
	case "hour", "day", "week", "month":
	default:
		return UsageRangeResult{}, fmt.Errorf("granularity must be hour, day, week, or month")
	}
	if count <= 0 {
		return UsageRangeResult{}, fmt.Errorf("count must be positive")
	}
	if offset < 0 {
		return UsageRangeResult{}, fmt.Errorf("offset must not be negative")
	}
	if count > usageRangeLimit(granularity) {
		return UsageRangeResult{}, fmt.Errorf("count exceeds %d %s points", usageRangeLimit(granularity), granularity)
	}
	now := truncateBeijing(time.Now(), granularity)
	to := addBeijingGranularity(now, granularity, 1-offset*count)
	from := addBeijingGranularity(to, granularity, -count)
	return m.UsageRangeExplicit(granularity, from, to)
}
