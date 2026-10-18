package httpx

import (
	"fmt"
	"runtime"
)

// Three identities coexist here; the boundary between them is deliberate and
// must not be widened:
//
//	UserAgent()          -> the /inference identity. Only lanes that actually
//	                        proxy a client request to an inference endpoint may
//	                        use it, because the free-tier gate inspects the
//	                        claimed client identity.
//	LegacyUserAgent()    -> the legacy /zen/v1 lane's v1.3.7 shape.
//	GatewayUserAgent()   -> requests the gateway issues as itself: model
//	                        catalog / capability / pricing fetches and proxy
//	                        health checks. Those are not inference traffic, so
//	                        claiming the opencode CLI identity there only
//	                        misleads the upstream and the local support log.

// clientVersion is the OpenCode release the gateway claims to be on the
// /inference lane. Upstream accepts a range of releases (the version number
// itself is not validated), so this only needs bumping when it starts rejecting
// older ones.
const clientVersion = "2.0.32"

// legacyClientVersion is the pre-2.x OpenCode release reported on the legacy
// /zen/v1 lane, matching the v1.3.7 User-Agent shape.
const legacyClientVersion = "1.18.31"

// gatewayVersion is the version the gateway claims for itself on its own,
// non-inference requests. Kept separate from clientVersion on purpose: bumping
// the impersonated OpenCode release must never move this string.
const gatewayVersion = "1.0"

// UserAgent reports the gateway as an OpenCode 2.x client using the shape the
// /inference gate expects: "opencode/<channel>/<version>/<name>". The previous
// "opencode/<version> (<os> <arch>; <runtime>)" form is rejected with 403
// FreeTierError even when the rest of the request is identical.
//
// This is the INFERENCE identity: only lanes that actually proxy a client
// request to an inference endpoint may use it, because the free-tier gate
// inspects the claimed client identity. Do not widen its use.
func UserAgent() string {
	return "opencode/latest/" + clientVersion + "/cli"
}

// LegacyUserAgent reports the gateway using the v1.3.7 OpenCode 1.18.x
// User-Agent shape ("opencode/<version> (<os> <arch>; <runtime>)") for the
// legacy /zen/v1 lane.
func LegacyUserAgent() string {
	return fmt.Sprintf("opencode/%s (%s %s; %s)", legacyClientVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// GatewayUserAgent is the User-Agent for requests the gateway issues as itself
// rather than as an OpenCode client: model catalog / capability / pricing
// fetches (internal/models) and proxy health checks (internal/gateway pool).
// Those are not inference traffic, so claiming the opencode CLI identity there
// only misleads the upstream (and the local support log); the free-tier gate
// never sees them, so self-identifying costs nothing. Same naming style as the
// admin package's "opencode2api-admin/1.0".
func GatewayUserAgent() string {
	return "opencode2api-gateway/" + gatewayVersion
}
