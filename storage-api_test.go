//
//  Basic testing of our storage layer.
//

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Test the list-function returns sensible results.
func TestList(t *testing.T) {
	//
	// Create a temporary directory.
	//
	p := t.TempDir()

	//
	// Init the filesystem storage-class
	//
	storage := new(FilesystemStorage)
	storage.Setup(p)

	//
	// Get the list of entries, which should be empty
	//
	list := storage.Existing()

	//
	// To start with our storage-path will be empty.
	//
	if len(list) != 0 {
		t.Errorf("Empty storage directory contains results!")
	}

	//
	// We're going to create some new files
	//
	files := []string{"steve", "test'", "foo"}

	//
	// Create each one.
	//
	for _, id := range files {
		//
		// By default these will not exist.
		//
		if storage.Exists(id) {
			t.Errorf("Exists(missing-file) succeeded!")
		}

		//
		// Create the file.
		//
		path := filepath.Join(p, id)
		content := []byte("File Content Here")
		_ = os.WriteFile(path, content, 0644)

		//
		// WHich should mean they exist.
		//
		if !storage.Exists(id) {
			t.Errorf("Failed to detect newly-created file")
		}
	}

	//
	// Get the updated entries beneath our storage-prefix.
	//
	list = storage.Existing()

	//
	// We should have exactly as many as in our list of filenames.
	//
	if len(list) != len(files) {
		t.Errorf("Added file entries, but they were not found - we should have %d", len(files))
	}

	//
	// Cleanup
	//
	_ = os.RemoveAll(p)
}

// Test the retrival-function returns sensible results.
func TestGet(t *testing.T) {
	//
	// Create a temporary directory.
	//
	p := t.TempDir()

	//
	// Init the filesystem storage-class
	//
	storage := new(FilesystemStorage)
	storage.Setup(p)

	//
	// We're going to create a couple of new files,
	// each file will have the same content as its filename.
	//
	files := []string{"steve", "test'", "foo"}

	//
	// Create each one.
	//
	for _, id := range files {
		//
		// Create the file.
		//
		path := filepath.Join(p, id)
		content := []byte(id)
		_ = os.WriteFile(path, content, 0644)
	}

	//
	// Now for each file attempt to retrieve the content
	//
	for _, id := range files {
		content, _ := storage.Get(id)
		stringContent := string(*content)

		if stringContent != id {
			t.Errorf("Content of '%s' was not '%s'",
				stringContent, id)
		}
	}

	//
	// Cleanup
	//
	_ = os.RemoveAll(p)
}

// Test the storage-function returns sensible results.
func TestStore(t *testing.T) {
	//
	// Create a temporary directory.
	//
	p := t.TempDir()

	//
	// Init the filesystem storage-class
	//
	storage := new(FilesystemStorage)
	storage.Setup(p)

	//
	// We're going to create a couple of new files,
	// each file will have the same content as its filename.
	//
	files := []string{"steve", "test'", "foo"}

	//
	// Create each one.
	//
	for _, id := range files {
		//
		// Meta-Data
		//
		meta := make(map[string]string)
		meta["filename"] = id

		//
		// File won't exist
		//
		if storage.Exists(id) {
			t.Errorf("Exists(missing-file) succeeded!")
		}

		//
		// Store it
		//
		storage.Store(id, []byte(id), meta)

		//
		// Now it should be present
		//
		if !storage.Exists(id) {
			t.Errorf("Exists(missing-file) succeeded!")
		}

		//
		// Retrieve it to ensure the meta-data matches
		//
		_, metaOut := storage.Get(id)
		if metaOut["filename"] != meta["filename"] {
			t.Errorf("meta-data mismatch after round-trip!")
		}
	}

	//
	// Cleanup
	//
	_ = os.RemoveAll(p)
}
