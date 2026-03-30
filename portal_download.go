package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// loadManifest fetches and parses the manifest.json for a given transfer ID.
// The manifest is stored in S3 at {prefix}{id}/manifest.json and contains
// everything we need to know about the transfer: files, expiry, message.
// Returns an error if the transfer doesn't exist or the JSON can't be parsed.
func loadManifest(ctx context.Context, cfg portalConfig, transferID string) (*transferManifest, error) {
	key := cfg.prefix + transferID + "/manifest.json"
	out, err := cfg.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	var m transferManifest
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// handleDownloadPage serves the download landing page for a transfer.
// It reads the transfer ID from the URL path, loads the manifest from S3,
// checks whether the transfer has expired, and renders the appropriate page.
// The {id} part of the URL is extracted automatically by Go's HTTP router.
func handleDownloadPage(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id") // extract the transfer ID from the URL, e.g. /d/abc123
		m, err := loadManifest(r.Context(), cfg, id)
		if err != nil {
			log.Printf("load manifest error for %q: %v", id, err)
			renderNotFoundPage(w)
			return
		}
		// Check expiry — the transfer might exist in S3 but no longer be valid
		if time.Now().UTC().After(m.ExpiresAt) {
			renderExpiredPage(w, m)
			return
		}
		renderDownloadPage(w, m)
	}
}

// handleDownloadFile streams a single file from S3 directly to the browser.
// It validates the transfer exists, hasn't expired, and that the requested
// filename is actually part of that transfer (preventing someone guessing keys).
// The file is streamed — it's never fully loaded into memory — so large files work fine.
func handleDownloadFile(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")           // transfer ID from URL
		filename := r.PathValue("filename") // filename from URL (URL-decoded automatically)

		// Load and validate the manifest before touching S3 for the file
		m, err := loadManifest(r.Context(), cfg, id)
		if err != nil {
			http.Error(w, "transfer not found", http.StatusNotFound)
			return
		}
		if time.Now().UTC().After(m.ExpiresAt) {
			http.Error(w, "transfer has expired", http.StatusGone)
			return
		}

		// Look up the requested filename in the manifest to get its S3 key.
		// We don't trust the filename from the URL to construct the key directly —
		// we always verify it exists in the manifest first.
		var fileKey string
		for _, f := range m.Files {
			if f.Name == filename {
				fileKey = f.Key
				break
			}
		}
		if fileKey == "" {
			http.Error(w, "file not found in transfer", http.StatusNotFound)
			return
		}

		// Fetch the file from S3. GetObject returns a streaming body —
		// the file is not downloaded all at once, just opened for reading.
		out, err := cfg.client.GetObject(r.Context(), &s3.GetObjectInput{
			Bucket: aws.String(cfg.bucket),
			Key:    aws.String(fileKey),
		})
		if err != nil {
			log.Printf("S3 GetObject error for %q: %v", fileKey, err)
			http.Error(w, "failed to retrieve file", http.StatusInternalServerError)
			return
		}
		defer out.Body.Close()

		// Content-Disposition tells the browser to download the file rather than
		// trying to display it. The filename*=UTF-8'' format handles non-ASCII
		// characters in filenames correctly across all modern browsers.
		w.Header().Set("Content-Disposition",
			fmt.Sprintf(`attachment; filename*=UTF-8''%s`, url.PathEscape(filename)))
		w.Header().Set("Content-Type", "application/octet-stream")
		// Providing Content-Length lets the browser show a progress bar
		if out.ContentLength != nil {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", *out.ContentLength))
		}

		// io.Copy streams the S3 response body directly to the HTTP response writer,
		// chunk by chunk. No matter how large the file is, memory usage stays low.
		if _, err := io.Copy(w, out.Body); err != nil {
			log.Printf("stream error for %q: %v", fileKey, err)
		}
	}
}
