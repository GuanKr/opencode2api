package httpx

// Three identities coexist here; the boundary between them is deliberate and
// must not be widened:
//
//	V2UserAgent()       -> the v2 lanes' inference identity (/inference and
//	                       /zen/go, both on the OpenCode 2.x CLI shape). Only
//	                       lanes that actually proxy a client request to an
//	                       inference endpoint may use it, because the free-tier
//	                       gate inspects the claimed client identity.
//	V1UserAgent() /     -> the v1 lane's (/zen) OpenCode 1.x identity, and the
//	V1ModelUserAgent()     AI SDK augmented form a real 1.x model request sends.
//	GatewayUserAgent()  -> requests the gateway issues as itself: model
//	                       catalog / capability / pricing fetches and proxy
//	                       health checks. Those are not inference traffic, so
//	                       claiming the opencode CLI identity there only
//	                       misleads the upstream and the local support log.

// v2ClientVersion is the OpenCode 2.x CLI release the gateway claims to be on
// the v2 lanes (/inference and /zen/go). Upstream accepts a range of releases
// (the version number itself is not validated), so this only needs bumping
// when it starts rejecting older ones.
const v2ClientVersion = "2.0.32"

// v1ClientVersion is the OpenCode 1.x release the gateway claims to be on the
// v1 lane (/zen), and v1BunVersion is the Bun runtime that release ships with.
// Bump them together with the provider-utils versions below, all taken from a
// captured request of the same release.
const (
	v1ClientVersion = "1.19.3"
	v1BunVersion    = "1.3.14"
)

// The AI SDK provider packages of OpenCode 1.19.3 each bundle their own
// provider-utils, whose version ends up in the User-Agent of every v1 model
// request, so the version depends on the wire protocol.
const (
	// V1ChatSDK is bundled by @ai-sdk/openai-compatible (chat completions).
	V1ChatSDK = "4.0.23"
	// V1ResponsesSDK is bundled by @ai-sdk/openai (responses).
	V1ResponsesSDK = "4.0.40"
	// V1AnthropicSDK is bundled by @ai-sdk/anthropic (messages).
	V1AnthropicSDK = "4.0.46"
)

// gatewayVersion is the version the gateway claims for itself on its own,
// non-inference requests. Kept separate from the client versions on purpose:
// bumping the impersonated OpenCode release must never move this string.
const gatewayVersion = "1.0"

// V2UserAgent reports the gateway as an OpenCode 2.x CLI using the shape the
// v2 gate expects: "opencode/<channel>/<version>/<name>". The previous
// "opencode/<version> (<os> <arch>; <runtime>)" form is rejected with 403
// FreeTierError even when the rest of the request is identical.
//
// This is the v2 INFERENCE identity: only lanes that actually proxy a client
// request to an inference endpoint may use it, because the free-tier gate
// inspects the claimed client identity. Do not widen its use.
func V2UserAgent() string {
	return "opencode/latest/" + v2ClientVersion + "/cli"
}

// V1UserAgent is the OpenCode 1.x client's own User-Agent, a bare
// "opencode/<version>", which it sends as-is on requests that bypass the AI
// SDK. The earlier "(<os> <arch>; <runtime>)" suffix carried the gateway's Go
// runtime, which no real OpenCode client sends.
func V1UserAgent() string {
	return "opencode/" + v1ClientVersion
}

// V1ModelUserAgent is the User-Agent of a v1 model request: the AI SDK appends
// the provider-utils version of the package serving the request and the
// runtime to the client's own User-Agent, e.g.
// "opencode/1.19.3 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14".
func V1ModelUserAgent(providerUtils string) string {
	return V1UserAgent() + " ai-sdk/provider-utils/" + providerUtils + " runtime/bun/" + v1BunVersion
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
