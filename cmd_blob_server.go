//
// Launch our blob-server.
//

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// storage holds a handle to our selected storage-method.
var storage StorageHandler

// setStorage stores the storage handler for use by handlers.
func setStorage(s StorageHandler) {
	storage = s
}

// getStorage returns the current storage handler.
func getStorage() StorageHandler {
	return storage
}

// HealthHandler is a status end-point which can be polled remotely
// to test health.
func HealthHandler(res http.ResponseWriter, _ *http.Request) {
	_, _ = res.Write([]byte("alive"))
}

// GetHandler allows a blob to be retrieved by name.
//
// This is called with requests like `GET /blob/XXXXXX`.
func GetHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)
		}
	}()

	//
	// Get the ID which is requested.
	//
	vars := mux.Vars(req)
	id := vars["id"]

	//
	// We're in a chroot() so we shouldn't need to worry
	// about relative paths.  That said the chroot() call
	// will have failed if we were not launched by root, so
	// we need to make sure we avoid directory-traversal attacks.
	//
	r := regexp.MustCompile("^([a-z0-9]+)$")
	if !r.MatchString(id) {
		status = http.StatusInternalServerError
		err = errors.New("alphanumeric IDs only")
		return
	}

	//
	// If the request method was HEAD we don't need to
	// lookup & return n the data, just see if it exists.
	//
	//  We'll terminate early and just return the status-code
	// 200 vs. 404.
	//
	if req.Method == http.MethodHead {
		res.Header().Set("Connection", "close")

		if !getStorage().Exists(id) {
			res.WriteHeader(http.StatusNotFound)
		}
		return
	}

	//
	// If we reached this point then the request was a GET
	// so we lookup the data, returning it if present.
	//
	data, meta := getStorage().Get(id)

	//
	// The data was missing..
	//
	if data == nil {
		http.NotFound(res, req)
	} else {
		//
		// The meta-data will be used to populate the HTTP-response
		// headers.
		//
		for k, v := range meta {
			//
			// Special case to set the content-type
			// of the returned value.
			//
			if k == "X-Mime-Type" {
				res.Header().Set(k, v)
				k = "Content-Type"
			}

			//
			// Add the response header.
			//
			res.Header().Set(k, v)
		}
		if _, copyErr := io.Copy(res, bytes.NewReader(*data)); copyErr != nil {
			panic(copyErr)
		}
	}
}

// MissingHandler is a handler which is used as a fall-back if no matching
// handler is found.
func MissingHandler(res http.ResponseWriter, _ *http.Request) {
	res.WriteHeader(http.StatusNotFound)
	if _, err := res.Write([]byte("404 - content is not hosted here.")); err != nil {
		panic(err)
	}
}

// ListHandler returns the IDs of all blobs we know about.
//
// This is used by the replication utility.
func ListHandler(res http.ResponseWriter, _ *http.Request) {
	list := getStorage().Existing()

	//
	// If the list is non-empty then build up an array
	// of the names, then send as JSON.
	//
	if len(list) > 0 {
		mapB, _ := json.Marshal(list)
		_, _ = res.Write(mapB)
	} else {
		_, _ = res.Write([]byte("[]"))
	}
}

// UploadHandler is invoked to handle storing data in the blob-server.
func UploadHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)
		}
	}()

	//
	// Get the name of the blob to upload.
	//
	// We've previously chdir() and chroot() to the upload
	// directory, so we don't need to worry about any path
	// issues - providing the user isn't trying a traversal
	// attack.
	//
	vars := mux.Vars(req)
	id := vars["id"]

	//
	// Ensure the ID is entirely alphanumeric, to prevent
	// traversal attacks.
	//
	r := regexp.MustCompile("^([a-z0-9]+)$")
	if !r.MatchString(id) {
		err = errors.New("alphanumeric IDs only")
		status = http.StatusInternalServerError
		return
	}

	//
	// Read the body of the request.
	//
	content, err := io.ReadAll(req.Body)
	if err != nil {
		err = errors.New("failed to read body")
		status = http.StatusInternalServerError
		return
	}

	//
	// If we received any X-headers in our request then save
	// them to our extra-hash.  These will be persisted and
	// restored
	//
	extras := make(map[string]string)

	for header, value := range req.Header {
		if strings.HasPrefix(header, "X-") {
			extras[header] = value[0]
		}
	}

	//
	// Store the body, via our interface.
	//
	if ok := getStorage().Store(id, content, extras); !ok {
		err = errors.New("failed to write to storage")
		status = http.StatusInternalServerError
		return
	}

	//
	// Output the result - horrid.
	//
	//  { "id": "foo",
	//   "size": 1234,
	//   "status": "ok",
	//  }
	//
	out := fmt.Sprintf("{\"id\":\"%s\",\"status\":\"OK\",\"size\":%d}", id, len(content))
	_, _ = res.Write([]byte(out))
}

// blobServer is our entry-point to the sub-command.
func blobServer(options blobServerCmd) {
	//
	// Create a storage system.
	//
	// At the moment we only have a filesystem-based storage
	// class.  In the future it is possible we'd have more, and we'd
	// choose between them via a command-line flag.
	//
	storageHandler := new(FilesystemStorage)
	storageHandler.Setup(options.store)
	setStorage(storageHandler)

	//
	// Create a new router and our route-mappings.
	//
	router := mux.NewRouter()
	router.HandleFunc("/alive", HealthHandler).Methods("GET")
	router.HandleFunc("/blob/{id}", GetHandler).Methods("GET")
	router.HandleFunc("/blob/{id}", GetHandler).Methods("HEAD")
	router.HandleFunc("/blob/{id}", UploadHandler).Methods("POST")
	router.HandleFunc("/blobs", ListHandler).Methods("GET")
	router.PathPrefix("/").HandlerFunc(MissingHandler)
	http.Handle("/", router)

	//
	// Launch the server
	//
	GetLogger().Info("blob-server starting",
		"url", "http://"+net.JoinHostPort(options.host, strconv.Itoa(options.port))+"/",
		"storage_path", options.store)

	server := &http.Server{
		Addr:         net.JoinHostPort(options.host, strconv.Itoa(options.port)),
		Handler:      nil,
		ReadTimeout:  serverReadTimeout,
		WriteTimeout: serverWriteTimeout,
		IdleTimeout:  serverIdleTimeout,
	}
	err := server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
