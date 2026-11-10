package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

// defaultTurnIdleTimeout is how long a turn may go with no frame at all from
// devin before the wrapper cancels it. On 2026-11-10 one ACP turn in four on
// devin 3000.12.43 stopped after its first tool call and sent nothing for five
// minutes, while the same task in `devin -p` finished in 9 seconds; without a
// watchdog that turn would hang the session forever.
const defaultTurnIdleTimeout = 10 * time.Minute

// cancelGracePeriod is how long session/cancel has to end the turn before the
// wrapper kills devin.
const cancelGracePeriod = 30 * time.Second

type Config struct {
	// DevinPath is the devin CLI to run (DEVIN_PATH, default "devin" on PATH).
	DevinPath string
	// UserConfigPath is devin's own user config, which the per-session config
	// starts from so the user's settings still apply
	// (DEVIN_USER_CONFIG, default ~/.config/devin/config.json).
	UserConfigPath string
	// TurnIdleTimeout: DEVIN_TURN_IDLE_TIMEOUT_SEC; 0 disables the watchdog.
	TurnIdleTimeout time.Duration
}

func loadConfig() Config {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("no home directory: %v", err)
	}
	cfg := Config{
		DevinPath:       envOr("DEVIN_PATH", "devin"),
		UserConfigPath:  envOr("DEVIN_USER_CONFIG", home+"/.config/devin/config.json"),
		TurnIdleTimeout: turnIdleTimeoutFromEnv(),
	}
	return cfg
}

// turnIdleTimeoutFromEnv reads DEVIN_TURN_IDLE_TIMEOUT_SEC. Unset uses the
// default; 0 disables the watchdog; anything unparseable or negative is a
// startup failure, never a silently disabled safety net.
func turnIdleTimeoutFromEnv() time.Duration {
	raw := os.Getenv("DEVIN_TURN_IDLE_TIMEOUT_SEC")
	if raw == "" {
		return defaultTurnIdleTimeout
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs < 0 {
		log.Fatalf("DEVIN_TURN_IDLE_TIMEOUT_SEC=%q is not a whole number of seconds >= 0", raw)
	}
	return time.Duration(secs) * time.Second
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
