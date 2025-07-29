package logging

import (
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
)

// InitLogger sets the log level and format based on the provided configuration
func InitLogger(cfg *config.Config) {
	// Set log level based on the log level from the config
	switch logLevel := cfg.LogLevel; logLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}

	// Optional: Customize the log format (e.g., JSONFormatter or TextFormatter)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}
