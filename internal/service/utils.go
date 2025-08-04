package service

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func configureLogging() {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
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
	configureLogging()
}