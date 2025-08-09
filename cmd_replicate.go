//
// Replicate objects between available blob-servers.
//

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/skx/sos/libconfig"
)

// Objects reads the list of objects on the given server.
func Objects(server string) []string {
	type listStrings []string
	var tmp listStrings

	//
	// Make the request to get the list of objects.
	//
	ctx := context.Background()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server+"/blobs", nil)
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		GetLogger().Error("Failed to get blobs", "error", err)
		os.Exit(1)
	}
	defer func() {
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				GetLogger().Error("Failed to close response body", "error", closeErr)
			}
		}
	}()

	//
	// Read the (JSON) response-body.
	//
	body, err := io.ReadAll(response.Body)
	if err != nil {
		GetLogger().Error("Failed to read response body", "error", err)
		return nil
	}

	//
	// Decode into an array of strings, and return it.
	//
	err = json.Unmarshal(body, &tmp)
	if err != nil {
		GetLogger().Error("Failed to unmarshal JSON", "error", err)
		return nil
	}
	return tmp
}

// HasObject tests if the specified server contains the given object.
func HasObject(server string, object string) bool {
	ctx := context.Background()
	request, _ := http.NewRequestWithContext(ctx, http.MethodHead, server+"/blob/"+object, nil)
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		GetLogger().Error("Error fetching object", "server", server, "object", object, "error", err)
		return false
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		GetLogger().Info("Object present", "object", object, "server", server)
		return true
	}

	GetLogger().Info("Object missing", "object", object, "server", server)
	return false
}

// MirrorObject attempts to replicate the specified object between the two
// listed hosts.
func MirrorObject(src string, dst string, obj string, options replicateCmd) bool {
	if options.verbose {
		GetLogger().Info("Mirroring object", "object", obj, "from", src, "to", dst)
	}

	//
	// Prepare to download the object.
	//
	srcURL := fmt.Sprintf("%s%s%s", src, "/blob/", obj)
	GetLogger().Info("Fetching object", "url", srcURL)

	ctx := context.Background()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, srcURL, nil)
	client := &http.Client{}
	response, err := client.Do(request)

	//
	// If there was an error we're done.
	//
	if err != nil {
		GetLogger().Error("Error fetching object", "object", obj, "src", src, "error", err)
		return false
	}
	defer response.Body.Close()

	//
	// Prepare to POST the body we've downloaded to
	// the mirror-location
	//
	dstURL := fmt.Sprintf("%s%s%s", dst, "/blob/", obj)
	GetLogger().Info("Uploading object", "url", dstURL)

	//
	// Build up a new request with context.
	//
	child, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, dstURL, response.Body)

	//
	// Copy any X-Header which was present
	// in our download to the mirror.
	//
	for header, value := range response.Header {
		if strings.HasPrefix(header, "X-") {
			child.Header.Set(header, value[0])
		}
	}

	//
	// Send the request.
	//
	client = &http.Client{}
	r, err := client.Do(child)
	if r != nil {
		defer r.Body.Close()
	}

	//
	// If there was no error we're good.
	//
	if err != nil {
		GetLogger().Error("Error sending object", "url", dstURL, "error", err)
		return false
	}

	return true
}

// SyncGroup syncs the contents of the specified hosts.
func SyncGroup(servers []libconfig.BlobServer, options replicateCmd) {
	//
	// If we're being verbose show the members
	//
	if options.verbose {
		for _, s := range servers {
			GetLogger().Info("Group member", "location", s.Location)
		}
	}

	//
	// For each server - download the content-list here
	//
	//   key is server-name
	//   val is array of strings
	//
	objects := make(map[string][]string)

	//
	//  Store the list of objects each server hosts in the
	// hash, keyed upon the server-location/name.
	//
	for _, s := range servers {
		objects[s.Location] = Objects(s.Location)
	}

	//
	// Right we have a list of servers.
	//
	// For each server we also have the list of objects
	// that they contain.
	//
	for _, server := range servers {
		//
		// The objects on this server
		//
		var obs = objects[server.Location]

		//
		// For each object.
		//
		for _, i := range obs {
			//
			//  Mirror the object to every server that is not itself
			//
			for _, mirror := range servers {
				//
				// Ensure that src != dst.
				//
				if mirror.Location != server.Location {
					// If the object is missing.
					if !HasObject(mirror.Location, i) {
						MirrorObject(server.Location, mirror.Location, i, options)
					}
				}
			}
		}
	}
}

// replicate is the entry-point to this sub-command.
func replicate(options replicateCmd) {
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
	// Show the blob-servers.
	//
	if options.verbose {
		GetLogger().Info("Blob servers listing")
		for _, entry := range libconfig.Servers() {
			GetLogger().Info("Blob server", "group", entry.Group, "location", entry.Location)
		}
	}

	//
	// Get a list of groups.
	//
	for _, entry := range libconfig.Groups() {
		if options.verbose {
			GetLogger().Info("Syncing group", "group", entry)
		}

		//
		// For each group, get the members, and sync them.
		//
		SyncGroup(libconfig.GroupMembers(entry), options)
	}
}
