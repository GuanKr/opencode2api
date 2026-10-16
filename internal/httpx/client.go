package httpx

// clientVersion is the OpenCode release the gateway claims to be. Upstream
// accepts a range of releases (the version number itself is not validated),
// so this only needs bumping when it starts rejecting older ones.
const clientVersion = "2.0.29"

// UserAgent reports the gateway as an OpenCode client using the shape upstream
// expects: "opencode/<channel>/<version>/<name>". The previous
// "opencode/<version> (<os> <arch>; <runtime>)" form is rejected with 403
// FreeTierError even when the rest of the request is identical.
//
// This is the INFERENCE identity: only lanes that actually proxy a client
// request to an inference endpoint may use it, because the free-tier gate
// inspects the claimed client identity. Do not widen its use.
func UserAgent() string {
	return "opencode/latest/" + clientVersion + "/cli"
}

// gatewayVersion is the version the gateway claims for itself on its own,
// non-inference requests. Kept separate from clientVersion on purpose: bumping
// the impersonated OpenCode release must never move this string.
const gatewayVersion = "1.0"

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
