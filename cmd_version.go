//
// Show our version - This uses a level of indirection for our test-case
//

package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
)

var (
	version = "unreleased"
)

// Show the version - using the provided writer.
func showVersion(options versionCmd) {
	showVersionWithWriter(options, os.Stdout)
}

// showVersionWithWriter shows the version using the specified writer.
func showVersionWithWriter(options versionCmd, writer io.Writer) {
	_, _ = fmt.Fprintf(writer, "%s\n", version)
	if options.verbose {
		_, _ = fmt.Fprintf(writer, "Built with %s\n", runtime.Version())
	}
}
