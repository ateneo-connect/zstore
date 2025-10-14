package logging

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
)

// InitLogger sets the log level and format based on the provided configuration
func InitLogger(cfg *config.Config) {
	setLogLevel(cfg.LogLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

// InitFromEnv initializes logging from environment variables
func InitFromEnv() {
	logLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))
	setLogLevel(logLevel)
}

// setLogLevel sets the log level based on string input
func setLogLevel(logLevel string) {
	switch logLevel {
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
}

func init() {
	InitFromEnv()
}
