package main

import (
	"bytes"
	"runtime"
	"testing"
)

func TestVersion(t *testing.T) {
	buffer := new(bytes.Buffer)

	//
	// Expected
	//
	expected := "unreleased\n"

	s := versionCmd{}
	showVersionWithWriter(s, buffer)
	if buffer.String() != expected {
		t.Errorf("Expected '%s' received '%s'", expected, buffer.String())
	}
}

func TestVersionVerbose(t *testing.T) {
	buffer := new(bytes.Buffer)

	//
	// Expected
	//
	expected := "unreleased\nBuilt with " + runtime.Version() + "\n"

	s := versionCmd{verbose: true}
	showVersionWithWriter(s, buffer)
	if buffer.String() != expected {
		t.Errorf("Expected '%s' received '%s'", expected, buffer.String())
	}
}
