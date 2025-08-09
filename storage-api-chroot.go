//go:build !windows
// +build !windows

package main

import (
	"syscall"
)

// SOSChroot attempts to call `chroot` with the given directory.
//
// This is not implemented for Windows.
func SOSChroot(directory string) {
	_ = syscall.Chroot(directory)
}
