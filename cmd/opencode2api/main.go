// Command opencode2api starts the API gateway and its management interface.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	adminui "opencode2api/internal/admin"
	"opencode2api/internal/buildinfo"
	"opencode2api/internal/config"
	"opencode2api/internal/gateway"
	"opencode2api/internal/telemetry"
)

// version remains the linker injection point used by release builds.
var version = "dev"

func main() {
	buildinfo.Version = version
	configPath := flag.String("config", "config.json", "path to config.json")
	listen := flag.String("listen", "", "override the configured API listen address")
	webListen := flag.String("web-listen", "", "override the configured WebUI listen address")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *webListen != "" {
		cfg.WebUI.Listen = *webListen
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	level := new(slog.LevelVar)
	telemetry.SetLogLevel(level, cfg.Logging.Level)
	hub := telemetry.NewLogHub(cfg.Logging.RingSize)
	redactor := config.NewSecretRedactor()
	redactor.Replace(cfg)
	logger := telemetry.NewStructuredLogger(level, hub, redactor)
	monitor := telemetry.NewMonitor()
	manager, err := gateway.NewRuntimeManager(ctx, *configPath, cfg, logger, monitor, hub, redactor, level)
	if err != nil {
		logger.Error("failed to initialize runtime", "component", "runtime", "event", "runtime_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer manager.Shutdown()
	// Local: usage history persistence (hour buckets beside config.json).
	usagePath := telemetry.UsageHistoryPath(*configPath)
	startUsageSaver(ctx, logger, monitor, usagePath)

	apiServer := &http.Server{
		Addr: cfg.Listen, Handler: manager.Handler(), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
	}
	servers := []*http.Server{apiServer}
	go serveHTTP(cancel, logger, apiServer, "api")

	if cfg.WebUI.Enabled {
		admin := adminui.New(manager, monitor, hub, logger)
		webServer := &http.Server{
			Addr: cfg.WebUI.Listen, Handler: admin.Handler(), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
		}
		servers = append(servers, webServer)
		go serveHTTP(cancel, logger, webServer, "webui")
	}

	<-ctx.Done()
	saveUsageHistory(logger, monitor, usagePath)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "component", "server", "event", "shutdown_failed", "address", server.Addr, "error", err)
		}
	}
}

// usageSaveInterval is how often the usage history is flushed. Every tick
// re-marshals the whole file (hour buckets up to 400 days plus the 24h detail
// ring), so the interval is the write-amplification knob: at steady state it
// multiplies the per-save cost. Five minutes keeps a crash's loss bounded to
// a few minutes of usage while cutting the write volume fivefold; a graceful
// shutdown flushes once more in main, so only a hard kill loses the tail.
const usageSaveInterval = 5 * time.Minute

// startUsageSaver persists hourly token-usage buckets beside config.json
// (0600, atomic rename) on the usageSaveInterval tick. Retention is 400 days;
// Beijing day/week/month roll up on query. SaveUsageHistory skips the
// snapshot+write when nothing changed (dirty flag), so idle ticks only run
// the cheap retention prune. Shutdown flushes once more in main.
//
// Save failures (unwritable directory, refused rename, the post-failure-load
// fence) are logged instead of dropped: without that, a disk that cannot be
// written freezes the history silently while the gateway looks healthy.
// One log line per tick at most — the ticker itself is the throttle.
func startUsageSaver(ctx context.Context, logger *slog.Logger, monitor *telemetry.Monitor, usagePath string) {
	go func() {
		ticker := time.NewTicker(usageSaveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				saveUsageHistory(logger, monitor, usagePath)
			}
		}
	}()
}

// saveUsageHistory flushes the usage history and reports a failure on the
// structured log. The logged fields are the path and the OS error only:
// usage history holds session ids and key tails, never credentials or
// request bodies.
func saveUsageHistory(logger *slog.Logger, monitor *telemetry.Monitor, usagePath string) {
	if err := monitor.SaveUsageHistory(usagePath); err != nil && logger != nil {
		logger.Warn("usage history save failed", "component", "usage", "event", "usage_history_save_failed", "path", usagePath, "error", err)
	}
}

func serveHTTP(cancel context.CancelFunc, logger *slog.Logger, server *http.Server, component string) {
	logger.Info("server listening", "component", component, "event", "server_started", "address", server.Addr, "version", buildinfo.Version)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server stopped unexpectedly", "component", component, "event", "server_failed", "address", server.Addr, "error", err)
		cancel()
	}
}
