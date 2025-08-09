package main

import (
	"bytes"
	"context"
	"runtime"
	"testing"
)

func TestVersion(t *testing.T) {
	bak := out
	out = new(bytes.Buffer)
	defer func() { out = bak }()

	//
	// Expected
	//
	expected := "unreleased\n"

	s := versionCmd{}
	ctx := context.Background()
	s.Execute(ctx, nil)
	if out.(*bytes.Buffer).String() != expected {
		t.Errorf("Expected '%s' received '%s'", expected, out)
	}
}

func TestVersionVerbose(t *testing.T) {
	bak := out
	out = new(bytes.Buffer)
	defer func() { out = bak }()

	//
	// Expected
	//
	expected := "unreleased\nBuilt with " + runtime.Version() + "\n"

	s := versionCmd{verbose: true}
	ctx := context.Background()
	s.Execute(ctx, nil)
	if out.(*bytes.Buffer).String() != expected {
		t.Errorf("Expected '%s' received '%s'", expected, out)
	}
}
