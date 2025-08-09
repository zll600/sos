//
// Launch our API-server.
//

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/skx/sos/libconfig"
)

// apiOptions holds options passed to this sub-command, so that we can later
// test if `-verbose` is in-force.
var apiOptions apiServerCmd

// setAPIOptions stores the API server options for use by handlers.
func setAPIOptions(opts apiServerCmd) {
	apiOptions = opts
}

// getAPIOptions returns the current API server options.
func getAPIOptions() apiServerCmd {
	return apiOptions
}

// Start the upload/download servers running.
func apiServer(options apiServerCmd) {
	//
	// If we received blob-servers on the command-line use them too.
	//
	// NOTE: blob-servers added on the command-line are placed in the
	// "default" group.
	//
	if options.blob != "" {
		servers := strings.SplitSeq(options.blob, ",")
		for entry := range servers {
			libconfig.AddServer("default", entry)
		}
	} else {
		//
		//  Initialize the servers from our config file(s).
		//
		libconfig.InitServers()
	}

	//
	// If we're merely dumping the servers then do so now.
	//
	if options.dump {
		GetLogger().Info("Blob servers", "group", "group", "server", "server")
		for _, entry := range libconfig.Servers() {
			GetLogger().Info("Blob server entry", "group", entry.Group, "location", entry.Location)
		}
		return
	}

	// Store options for later use by handlers
	setAPIOptions(options)

	//
	// Otherwise show a banner, then launch the server-threads.
	//
	GetLogger().Info("Launching API-server")
	GetLogger().Info(
		"Upload service",
		"url",
		"http://"+net.JoinHostPort(options.host, strconv.Itoa(options.uport))+"/upload",
	)
	GetLogger().Info(
		"Download service",
		"url",
		"http://"+net.JoinHostPort(options.host, strconv.Itoa(options.dport))+"/fetch/:id",
	)

	//
	// Show the blob-servers, and their weights
	//
	GetLogger().Info("Blob-servers:")
	for _, entry := range libconfig.Servers() {
		GetLogger().Info("Blob server", "group", entry.Group, "location", entry.Location)
	}

	//
	// Create a route for uploading.
	//
	upRouter := mux.NewRouter()
	upRouter.HandleFunc("/upload", APIUploadHandler).Methods("POST")
	upRouter.PathPrefix("/").HandlerFunc(APIMissingHandler)

	//
	// Create a route for downloading.
	//
	downRouter := mux.NewRouter()
	downRouter.HandleFunc("/fetch/{id}", APIDownloadHandler).Methods("GET")
	downRouter.HandleFunc("/fetch/{id}", APIDownloadHandler).Methods("HEAD")
	downRouter.PathPrefix("/").HandlerFunc(APIMissingHandler)

	//
	// The following code is a hack to allow us to run two distinct
	// HTTP-servers on different ports.
	//
	// It's almost sexy the way it worked the first time :)
	//
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server := &http.Server{
			Addr:         net.JoinHostPort(options.host, strconv.Itoa(options.uport)),
			Handler:      upRouter,
			ReadTimeout:  serverReadTimeout,
			WriteTimeout: serverWriteTimeout,
			IdleTimeout:  serverIdleTimeout,
		}
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		server := &http.Server{
			Addr:         net.JoinHostPort(options.host, strconv.Itoa(options.dport)),
			Handler:      downRouter,
			ReadTimeout:  serverReadTimeout,
			WriteTimeout: serverWriteTimeout,
			IdleTimeout:  serverIdleTimeout,
		}
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	wg.Wait()
}

// This is a helper for allowing us to consume a HTTP-body more than once.
type myReader struct {
	*bytes.Buffer
}

// So that it implements the io.ReadCloser interface.
func (m myReader) Close() error { return nil }

// APIUploadHandler handles uploads to the API server.
//
// This should attempt to upload against the blob-servers and return
// when that is complete.  If there is a failure then it should
// repeat the process until all known servers are exhausted.
//
// The retry logic is described in the file `SCALING.md` in the
// repository, but in brief there are two cases:
//
//   - All the servers are in the group `default`.
//
//   - There are N defined groups.
//
// Both cases are handled by the call to OrderedServers() which
// returns the known blob-servers in a suitable order to minimize
// lookups.  See `SCALING.md` for more details.
func APIUploadHandler(res http.ResponseWriter, req *http.Request) {
	//
	// We create a new buffer to hold the request-body.
	//
	buf, _ := io.ReadAll(req.Body)

	//
	// Create a copy of the buffer, so that we can consume
	// it initially to hash the data.
	//
	rdr1 := myReader{bytes.NewBuffer(buf)}

	//
	// Get the SHA256 hash of the uploaded data.
	//
	hasher := sha256.New()
	b, _ := io.ReadAll(rdr1)
	hasher.Write(b)
	hash := hasher.Sum(nil)

	//
	// Now we're going to attempt to re-POST the uploaded
	// content to one of our blob-servers.
	//
	// We try each blob-server in turn, and if/when we receive
	// a successful result we'll return it to the caller.
	//
	for _, s := range libconfig.OrderedServers() {
		//
		// Replace the request body with the (second) copy we made.
		//
		rdr2 := myReader{bytes.NewBuffer(buf)}
		req.Body = rdr2

		//
		// This is where we'll POST to.
		//
		url := fmt.Sprintf("%s%s%x", s.Location, "/blob/", hash)

		//
		// Build up a new request with context.
		//
		child, _ := http.NewRequestWithContext(req.Context(), http.MethodPost, url, req.Body)

		//
		// Propagate any incoming X-headers
		//
		for header, value := range req.Header {
			if strings.HasPrefix(header, "X-") {
				child.Header.Set(header, value[0])
			}
		}

		//
		// Send the request.
		//
		client := &http.Client{}
		r, err := client.Do(child)
		if r != nil {
			defer r.Body.Close()
		}

		//
		// If there was no error we're good.
		//
		if err == nil {
			//
			// We read the reply we received from the
			// blob-server and return it to the caller.
			//
			response, _ := io.ReadAll(r.Body)

			if response != nil {
				if _, writeErr := res.Write(response); writeErr != nil {
					panic(writeErr)
				}
				return
			}
		}
	}

	//
	// If we reach here we've attempted our upload on every
	// known blob-server and none accepted it.
	//
	// Let the caller know.
	//
	res.WriteHeader(http.StatusInternalServerError)
	if _, err := res.Write([]byte("{\"error\":\"upload failed\"}")); err != nil {
		panic(err)
	}
}

// logDownloadError logs error details when verbose mode is enabled.
func logDownloadError(err error, response *http.Response) {
	if !getAPIOptions().verbose {
		return
	}

	if err != nil {
		GetLogger().Error("Error fetching", "error", err.Error())
	} else if response != nil {
		GetLogger().Warn("Non-200 status code", "status_code", response.StatusCode)
	}
}

// handleSuccessfulDownload processes a successful response from a blob server.
func handleSuccessfulDownload(res http.ResponseWriter, req *http.Request, response *http.Response) bool {
	body, _ := io.ReadAll(response.Body)

	if body == nil {
		return false
	}

	if getAPIOptions().verbose {
		GetLogger().Info("Found data", "bytes", len(body))
	}

	// Handle HEAD requests
	if req.Method == http.MethodHead {
		res.Header().Set("Connection", "close")
		res.WriteHeader(http.StatusOK)
		return true
	}

	// Copy X-Headers from the response
	for header, value := range response.Header {
		if strings.HasPrefix(header, "X-") {
			res.Header().Set(header, value[0])
		}
	}

	// Send back the body
	if _, copyErr := io.Copy(res, bytes.NewReader(body)); copyErr != nil {
		panic(copyErr)
	}
	return true
}

// tryDownloadFromServer attempts to download from a single blob server.
func tryDownloadFromServer(server libconfig.BlobServer, id string, res http.ResponseWriter, req *http.Request) bool {
	if getAPIOptions().verbose {
		GetLogger().Info("Attempting retrieval", "url", fmt.Sprintf("%s%s%s", server.Location, "/blob/", id))
	}

	ctx := context.Background()
	request, _ := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s%s%s", server.Location, "/blob/", id),
		nil,
	)
	client := &http.Client{}
	response, err := client.Do(request)
	if response != nil {
		defer response.Body.Close()
	}

	if err != nil || response == nil || response.StatusCode != http.StatusOK {
		logDownloadError(err, response)
		return false
	}

	return handleSuccessfulDownload(res, req, response)
}

// APIDownloadHandler handles downloads from the API server.
//
// This should attempt to download against the blob-servers and return
// when that is complete.  If there is a failure then it should
// repeat the process until all known servers are exhausted..
//
// The retry logic is described in the file `SCALING.md` in the
// repository, but in brief there are two cases:
//
//   - All the servers are in the group `default`.
//
//   - There are N defined groups.
//
// Both cases are handled by the call to OrderedServers() which
// returns the known blob-servers in a suitable order to minimize
// lookups.  See `SCALING.md` for more details.
func APIDownloadHandler(res http.ResponseWriter, req *http.Request) {
	// Extract ID from request
	vars := mux.Vars(req)
	id := vars["id"]

	// Strip any extension which might be present on the ID
	extension := filepath.Ext(id)
	id = id[0 : len(id)-len(extension)]

	// Try each blob-server in turn
	for _, server := range libconfig.OrderedServers() {
		if tryDownloadFromServer(server, id, res, req) {
			return
		}
	}

	// If we reach here, no server succeeded
	res.Header().Set("Connection", "close")
	res.WriteHeader(http.StatusNotFound)
}

// APIMissingHandler is a fall-back handler for all requests which are
// neither upload nor download.
func APIMissingHandler(res http.ResponseWriter, _ *http.Request) {
	res.WriteHeader(http.StatusNotFound)
	if _, err := res.Write([]byte("Invalid method or location.")); err != nil {
		panic(err)
	}
}
