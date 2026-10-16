package gateway

import "testing"

// isModelUnavailable is pure: it never touches the network, so these cases
// need neither freezeProxyHealth nor a stub upstream. The pair that matters is
// the first two — an echoed "unknown model" phrase must stay available, while a
// structured model_not_found code must still take the model out of service.
func TestIsModelUnavailableIgnoresProseEcho(t *testing.T) {
	// A 4xx that quotes the request back. The prose appears in the echoed
	// message list and in error.message, never in a machine-readable field.
	echoed := `{"error":{"type":"invalid_request_error","message":"bad request: unknown model in prompt",` +
		`"request":{"model":"grok-free","messages":[{"role":"user",` +
		`"content":"please explain what an unknown model is and compare it to a model that does not exist"}]}}}`
	if isModelUnavailable(400, echoed) {
		t.Fatal("prose 'unknown model' echoed from the request must not mark the model unavailable")
	}

	// Same prose, but as the entire non-JSON body: still not a machine code.
	if isModelUnavailable(404, "model does not exist") {
		t.Fatal("prose-only non-JSON body must not mark the model unavailable")
	}

	// A machine code buried below the inspected fields is an echo too: here the
	// user pasted a previous error log into the prompt. Only error.type / code /
	// message and the top-level trio carry the verdict, so a scan of the whole
	// document — JSON-scoped or not — would wrongly disable the model.
	nested := `{"error":{"type":"invalid_request_error","message":"prompt rejected",` +
		`"request":{"messages":[{"role":"user","content":"fix this: {\"error\":{\"code\":\"model_not_found\"}}"}]}}}`
	if isModelUnavailable(400, nested) {
		t.Fatal("machine code inside an echoed request must not mark the model unavailable")
	}
}

func TestIsModelUnavailableMatchesStructuredCodes(t *testing.T) {
	cases := map[string]string{
		"error.type":     `{"error":{"type":"model_not_found","message":"not available"}}`,
		"error.code":     `{"error":{"type":"invalid_request_error","code":"model_retired"}}`,
		"error.message":  `{"error":{"message":"upstream said model_not_available"}}`,
		"top type":       `{"type":"model_deprecated","message":"gone"}`,
		"top code":       `{"code":"model_not_found"}`,
		"top message":    `{"message":"MODEL_NOT_FOUND"}`,
		"plain text":     "model_not_found",
		"plain text 404": "Error: model_retired\n",
	}
	for name, body := range cases {
		status := 400
		if name == "plain text 404" {
			status = 404
		}
		if !isModelUnavailable(status, body) {
			t.Fatalf("%s: expected unavailable for %s", name, body)
		}
	}
}

// The status gate is unchanged: a machine code on a 5xx or a 2xx is not a
// verdict about the model's existence.
func TestIsModelUnavailableStatusGate(t *testing.T) {
	for _, status := range []int{200, 401, 403, 429, 500, 503} {
		if isModelUnavailable(status, `{"error":{"type":"model_not_found"}}`) {
			t.Fatalf("status %d must not disable the model", status)
		}
	}
	for _, status := range []int{400, 404, 410, 422} {
		if !isModelUnavailable(status, `{"error":{"type":"model_not_found"}}`) {
			t.Fatalf("status %d should accept a structured model_not_found", status)
		}
	}
}

// isEffortRejection keeps its prose matching on purpose: unlike model
// availability, the wording *is* the signal there. Pinned so a future
// narrowing pass cannot silently apply the model rule to it.
func TestIsEffortRejectionStillMatchesProse(t *testing.T) {
	body := `{"error":{"message":"reasoning effort is not supported for this model"}}`
	if !isEffortRejection(400, body) {
		t.Fatal("isEffortRejection must keep matching prose rejection wording")
	}
	if isEffortRejection(400, `{"error":{"message":"unknown model"}}`) {
		t.Fatal("isEffortRejection must not fire on prose without effort/reasoning wording")
	}
}
