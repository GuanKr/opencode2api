package admin

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"opencode2api/internal/httpx"
	"opencode2api/internal/telemetry"
)

// Local: usage API (GET /api/usage range query).
// This file owns the usage range endpoint. server.go keeps only the route
// registration hook; the JSON shape and query contract are unchanged.

// handleUsage serves Beijing-aligned token usage for hour/day/week/month.
// Query forms: ?granularity=day&count=30&offset=0 (paged latest) or
// ?granularity=day&from=2026-09-01&to=2026-09-06 (explicit, RFC3339 or
// YYYY-MM-DD interpreted as Beijing midnight; a bare YYYY-MM-DD "to" names
// a whole calendar day and is inclusive, while an RFC3339 "to" stays the
// exact instant). Costs are USD estimated at record time with models.dev
// prices; unknown models record zero cost.
func (a *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	granularity := strings.ToLower(strings.TrimSpace(query.Get("granularity")))
	if granularity == "" {
		granularity = "day"
	}
	switch granularity {
	case "hour", "day", "week", "month":
	default:
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "granularity must be hour, day, week, or month")
		return
	}
	fromRaw := strings.TrimSpace(query.Get("from"))
	toRaw := strings.TrimSpace(query.Get("to"))
	var result telemetry.UsageRangeResult
	var err error
	if fromRaw != "" || toRaw != "" {
		if fromRaw == "" || toRaw == "" {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "from and to must be provided together")
			return
		}
		from, _, parseErr := parseUsageTime(fromRaw)
		if parseErr != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "invalid from: "+parseErr.Error())
			return
		}
		to, toBare, parseErr := parseUsageTime(toRaw)
		if parseErr != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "invalid to: "+parseErr.Error())
			return
		}
		// The range is [from, to): a bare YYYY-MM-DD "to" parses as that
		// day's midnight and would silently exclude the named day itself,
		// so only a bare "to" advances one day. Precise RFC3339 instants
		// are never shifted, and "from" never is either (it is inclusive).
		if toBare {
			to = to.AddDate(0, 0, 1)
		}
		result, err = a.monitor.UsageRangeExplicit(granularity, from, to)
	} else {
		count := defaultUsageCount(granularity)
		if raw := strings.TrimSpace(query.Get("count")); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed <= 0 {
				writeAdminError(w, http.StatusBadRequest, "invalid_request", "count must be a positive integer")
				return
			}
			count = parsed
		}
		offset := 0
		if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed < 0 {
				writeAdminError(w, http.StatusBadRequest, "invalid_request", "offset must not be negative")
				return
			}
			offset = parsed
		}
		result, err = a.monitor.UsageRangeCount(granularity, count, offset)
	}
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, result)
}

// defaultUsageCount returns the default page size per granularity.
func defaultUsageCount(granularity string) int {
	switch granularity {
	case "hour":
		return 24
	case "day":
		return 30
	case "week":
		return 12
	case "month":
		return 12
	default:
		return 30
	}
}

// parseUsageTime accepts RFC3339 or YYYY-MM-DD (Beijing midnight). It also
// reports whether the input was a bare calendar date: only a bare "to" date
// gets end-of-range expansion in handleUsage, never a precise timestamp.
func parseUsageTime(raw string) (time.Time, bool, error) {
	if value, err := time.Parse(time.RFC3339, raw); err == nil {
		return value, false, nil
	}
	if value, err := time.ParseInLocation("2006-01-02", raw, telemetry.BeijingZone); err == nil {
		return value, true, nil
	}
	return time.Time{}, false, fmt.Errorf("use RFC3339 or YYYY-MM-DD")
}
