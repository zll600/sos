//
// Centralized logger configuration for the SOS application.
//

package main

import (
	"log/slog"
)

// logger is the centralized logger instance for the application.
var logger *slog.Logger

// init initializes the logger with default configuration.
func initLogger() {
	logger = slog.Default()
}

func GetLogger() *slog.Logger {
	return logger
}
