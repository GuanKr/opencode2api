package telemetry

// Local: usage cost (record-time models.dev price estimation).
// This file owns ModelPrice/SetPriceFunc/usageCostLocked/recordHourUsage.
// metrics.go keeps the minute-bucket Monitor skeleton; usage_history.go owns
// hour buckets and persistence; usage_range.go owns range aggregation.

import (
	"sort"
	"time"
)

// ModelPrice carries models.dev per-1M-token prices for cost estimation.
// Missing Input/Output sides stay 0 so partially-known models still price
// their known side instead of rounding the whole request to $0. Missing
// cache sides fall back to the base input rate (applied by the price-func
// provider); models.dev simply omits them when it has no cache pricing.
type ModelPrice struct {
	Input     float64
	Output    float64
	CacheRead float64
}

// SetPriceFunc supplies current models.dev prices (USD per 1M tokens) for
// historical cost estimation. It is set once by RuntimeManager; the function
// itself reads the metadata store so price updates apply to later records.
func (m *Metrics) SetPriceFunc(fn func(string) (ModelPrice, bool)) {
	m.mu.Lock()
	m.priceFunc = fn
	m.mu.Unlock()
}

// usageCostLocked estimates USD cost for tokens with the price func captured at
// record time. The caller must hold m.mu. It also reports whether the model
// price is known: unknown models record zero cost (but keep tokens) and set
// the bucket UnknownCost flag so the UI can tell "unknown price" ($0 with a
// marker) apart from a known-free model (plain $0).
//
// Pricing: fresh input (Input-Cached) at the input rate, cache reads at the
// models.dev cache-read rate (falling back to the input rate when omitted),
// full Output (reasoning included, it is a subset of Output) at the output
// rate. The min clamp only defends against malformed upstream counts
// (Cached > Input); well-formed data keeps the subset invariant and prices
// exactly once. Do not "fix" the clamp into an exact-partition assumption.
func (m *Metrics) usageCostLocked(model string, tokens TokenCounts) (float64, bool) {
	if m.priceFunc == nil || model == "" {
		return 0, false
	}
	price, ok := m.priceFunc(model)
	if !ok {
		return 0, false
	}
	cached := min(tokens.Cached, tokens.Input)
	fresh := tokens.Input - cached
	return (float64(fresh)*price.Input + float64(cached)*price.CacheRead + float64(tokens.Output)*price.Output) / 1e6, true
}

// addCostMap accumulates a non-zero cost under a non-empty key.
func addCostMap(target map[string]float64, key string, value float64) {
	if key == "" || value == 0 {
		return
	}
	target[key] += value
}

// recordHourUsage folds one inference request into the current UTC hour bucket.
// The caller must hold m.mu. Cost uses the price func captured at record time
// and is returned so Monitor.Record can reuse it for the per-request entry.
func (m *Metrics) recordHourUsage(now time.Time, endpoint string, meta *RequestMeta, tokens TokenCounts) float64 {
	now = now.UTC()
	hour := now.Truncate(time.Hour).Unix()
	if m.usageHours == nil {
		m.usageHours = make(map[int64]*usageHourBucket)
	}
	bucket, ok := m.usageHours[hour]
	if !ok {
		bucket = newUsageHourBucket(hour)
		m.usageHours[hour] = bucket
	}
	if bucket.Models == nil {
		bucket.Models = make(map[string]TokenCounts)
	}
	if bucket.Tiers == nil {
		bucket.Tiers = make(map[string]TokenCounts)
	}
	if bucket.ModelCosts == nil {
		bucket.ModelCosts = make(map[string]float64)
	}
	if bucket.TierCosts == nil {
		bucket.TierCosts = make(map[string]float64)
	}
	if bucket.Endpoints == nil {
		bucket.Endpoints = make(map[string]uint64)
	}
	if bucket.EndpointTokens == nil {
		bucket.EndpointTokens = make(map[string]TokenCounts)
	}
	if bucket.EndpointCosts == nil {
		bucket.EndpointCosts = make(map[string]float64)
	}
	bucket.Requests++
	if meta.UsageReported {
		bucket.Reported++
	}
	addTokenCounts(&bucket.Tokens, tokens)
	addTokenMap(bucket.Models, meta.Model, tokens)
	addTokenMap(bucket.Tiers, meta.Tier, tokens)
	if len(bucket.Models) > usageHistoryMaxModels {
		collapseSmallestTokenMap(bucket.Models, bucket.ModelCosts, usageHistoryMaxModels)
	}
	cost, known := m.usageCostLocked(meta.Model, tokens)
	if !known {
		bucket.UnknownCost = true
	}
	if cost > 0 {
		bucket.CostUSD += cost
		addCostMap(bucket.ModelCosts, meta.Model, cost)
		addCostMap(bucket.TierCosts, meta.Tier, cost)
	}
	if endpoint != "" {
		bucket.Endpoints[endpoint]++
		addTokenMap(bucket.EndpointTokens, endpoint, tokens)
		addCostMap(bucket.EndpointCosts, endpoint, cost)
	}
	if len(m.usageHours) > usageHistoryRetentionDays*24+48 {
		m.pruneUsageHoursLocked(now)
	}
	return cost
}

// collapseSmallestTokenMap folds the smallest models into "_other" so the
// per-bucket model cardinality stays bounded.
func collapseSmallestTokenMap(target map[string]TokenCounts, costs map[string]float64, keep int) {
	if len(target) <= keep {
		return
	}
	type entry struct {
		key   string
		total uint64
	}
	entries := make([]entry, 0, len(target))
	for key, value := range target {
		entries = append(entries, entry{key: key, total: value.Total})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].total < entries[j].total })
	overflow := TokenCounts{}
	overflowCost := 0.0
	for _, item := range entries[:len(entries)-keep] {
		addTokenCounts(&overflow, target[item.key])
		overflowCost += costs[item.key]
		delete(target, item.key)
		delete(costs, item.key)
	}
	if overflow.Total > 0 {
		current := target["_other"]
		addTokenCounts(&current, overflow)
		target["_other"] = current
		if costs != nil && overflowCost > 0 {
			costs["_other"] += overflowCost
		}
	}
}
