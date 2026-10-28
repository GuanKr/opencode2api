package config

import "testing"

// promptEnvConfig builds the smallest valid configuration Normalize accepts, so
// each case below can vary prompt_env alone.
func promptEnvConfig(env PromptEnvConfig) Config {
	return Config{
		Listen:      "127.0.0.1:8080",
		ServerKeys:  []string{"local-key"},
		Anonymous:   true,
		Proxies:     []string{"direct"},
		Upstream:    UpstreamConfig{Zen: "https://opencode.ai/zen", Go: "https://opencode.ai/zen/go", Anonymous: "https://opencode.ai/inference"},
		PromptEnv:   env,
		Retry:       RetryConfig{MaxAttempts: 3, TimeoutSeconds: 300},
		Models:      ModelsConfig{RefreshSeconds: 300, Protocols: map[string]string{}},
		Performance: PerformanceConfig{MaxIdleConns: 10, MaxIdleConnsPerHost: 10, IdleConnTimeoutSeconds: 30, ConnectTimeoutSeconds: 5, FailureCooldownSeconds: 15},
		Logging:     LoggingConfig{Level: "info", RingSize: 100},
		Prefer:      TierGo,
	}
}

// An absent prompt_env must not leave empty directory lines in the injected
// <env> block; it falls back to the official CLI container path.
func TestNormalizePromptEnvDefaultsToTheContainerPath(t *testing.T) {
	cfg, err := Normalize("config.json", promptEnvConfig(PromptEnvConfig{}))
	if err != nil {
		t.Fatalf("Normalize rejected an absent prompt_env: %v", err)
	}
	if cfg.PromptEnv.WorkingDirectory != DefaultPromptWorkingDirectory {
		t.Fatalf("working_directory = %q, want %q", cfg.PromptEnv.WorkingDirectory, DefaultPromptWorkingDirectory)
	}
	if cfg.PromptEnv.WorkspaceRoot != DefaultPromptWorkingDirectory {
		t.Fatalf("workspace_root = %q, want it to follow the default working directory", cfg.PromptEnv.WorkspaceRoot)
	}
}

// A configured working directory is trimmed and reported verbatim; the root
// follows it when the operator leaves the root empty.
func TestNormalizePromptEnvTrimsAndFollowsTheWorkingDirectory(t *testing.T) {
	cfg, err := Normalize("config.json", promptEnvConfig(PromptEnvConfig{WorkingDirectory: "  D:\\work  "}))
	if err != nil {
		t.Fatalf("Normalize rejected prompt_env: %v", err)
	}
	if cfg.PromptEnv.WorkingDirectory != "D:\\work" {
		t.Fatalf("working_directory = %q, want %q", cfg.PromptEnv.WorkingDirectory, "D:\\work")
	}
	if cfg.PromptEnv.WorkspaceRoot != cfg.PromptEnv.WorkingDirectory {
		t.Fatalf("workspace_root = %q, want it to follow %q", cfg.PromptEnv.WorkspaceRoot, cfg.PromptEnv.WorkingDirectory)
	}
}

// The two values are independent: a configured workspace root survives a
// different working directory.
func TestNormalizePromptEnvKeepsTheConfiguredWorkspaceRoot(t *testing.T) {
	cfg, err := Normalize("config.json", promptEnvConfig(PromptEnvConfig{WorkingDirectory: "/srv/agent", WorkspaceRoot: " /srv/workspace "}))
	if err != nil {
		t.Fatalf("Normalize rejected prompt_env: %v", err)
	}
	if cfg.PromptEnv.WorkingDirectory != "/srv/agent" || cfg.PromptEnv.WorkspaceRoot != "/srv/workspace" {
		t.Fatalf("prompt_env = %+v, want the configured pair", cfg.PromptEnv)
	}
}

// A newline would break the <env> block's one-line shape, so it is a startup
// error rather than silently rendered.
func TestNormalizePromptEnvRejectsLineBreaks(t *testing.T) {
	cases := map[string]PromptEnvConfig{
		"working_directory": {WorkingDirectory: "/a\n/b"},
		"workspace_root":    {WorkingDirectory: "/a", WorkspaceRoot: "/b\r/c"},
	}
	for name, env := range cases {
		if _, err := Normalize("config.json", promptEnvConfig(env)); err == nil {
			t.Fatalf("%s: Normalize accepted a line break", name)
		}
	}
}
