package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// transferManifest is the metadata record for a single transfer.
// It is serialised to JSON and stored in S3 alongside the uploaded files,
// acting as the source of truth for the transfer — no database required.
type transferManifest struct {
	ID        string         `json:"id"`         // unique transfer identifier (UUID-style)
	CreatedAt time.Time      `json:"created_at"` // when the transfer was created
	ExpiresAt time.Time      `json:"expires_at"` // when the transfer link stops working
	Message   string         `json:"message,omitempty"` // optional message from the sender
	Files     []transferFile `json:"files"`      // list of files included in this transfer
}

// transferFile records the details of one file within a transfer.
// The Key field is the full S3 object key, which is what we use to retrieve it later.
type transferFile struct {
	Name string `json:"name"` // original filename as uploaded
	Size int64  `json:"size"` // file size in bytes
	Key  string `json:"key"`  // full S3 key where the file is stored
}

// newTransferID generates a random 16-byte ID formatted as a UUID-style string.
// Using crypto/rand ensures it's cryptographically random and practically unguessable,
// which matters because the ID is the only thing protecting a transfer link.
func newTransferID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// handleUploadPage serves the main upload UI — the drag-and-drop page.
// It returns an http.HandlerFunc (a function) rather than being a handler itself,
// so that it can close over the cfg variable and use it when requests arrive.
func handleUploadPage(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderUploadPage(w, uploadPageData{ExpiryHours: cfg.expiryHours})
	}
}

// handleUpload processes a file upload form submission. It:
//  1. Enforces the configured size limit on the incoming request body
//  2. Parses the multipart form (files + optional message)
//  3. Validates each filename for safety
//  4. Uploads each file to S3 under transfers/{uuid}/{filename}
//  5. Writes a manifest.json to S3 with the transfer metadata
//  6. Redirects the sender to the download page for their new transfer
func handleUpload(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Wrap the request body with a size limiter so oversized uploads are rejected
		// before they consume memory or disk. MaxBytesReader returns an error if
		// more than maxUploadSize bytes are read.
		r.Body = http.MaxBytesReader(w, r.Body, cfg.maxUploadSize)
		// ParseMultipartForm reads the form data. Files larger than 32MB are
		// written to temporary files on disk rather than held in memory.
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			renderUploadPage(w, uploadPageData{
				ExpiryHours: cfg.expiryHours,
				Error:       "Upload exceeds size limit or is malformed.",
			})
			return
		}

		// "files" matches the name attribute on the file input in the HTML form
		files := r.MultipartForm.File["files"]
		if len(files) == 0 {
			renderUploadPage(w, uploadPageData{
				ExpiryHours: cfg.expiryHours,
				Error:       "Please select at least one file.",
			})
			return
		}

		transferID, err := newTransferID()
		if err != nil {
			http.Error(w, "internal error generating transfer ID", http.StatusInternalServerError)
			return
		}

		// Trim whitespace from the optional message so we don't store blank strings
		message := strings.TrimSpace(r.FormValue("message"))
		now := time.Now().UTC()

		// Build the manifest that will be saved to S3. Files are appended below
		// as each one is successfully uploaded.
		manifest := transferManifest{
			ID:        transferID,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Duration(cfg.expiryHours) * time.Hour),
			Message:   message,
		}

		for _, fh := range files {
			name := fh.Filename

			// Reject filenames that could cause problems:
			// - "manifest.json" is reserved for transfer metadata
			// - slashes would let someone write outside the transfer's folder
			// - ".." could be used to traverse up the directory tree
			if name == "manifest.json" || strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
				renderUploadPage(w, uploadPageData{
					ExpiryHours: cfg.expiryHours,
					Error:       fmt.Sprintf("Invalid filename: %q", name),
				})
				return
			}

			// Open gives us a readable handle to the file — either from memory
			// or from a temp file on disk, depending on file size
			f, err := fh.Open()
			if err != nil {
				http.Error(w, "failed to read uploaded file", http.StatusInternalServerError)
				return
			}

			// S3 key format: transfers/{uuid}/{filename}
			// This keeps all files for a transfer grouped under the same prefix
			key := cfg.prefix + transferID + "/" + name
			_, uploadErr := cfg.client.PutObject(r.Context(), &s3.PutObjectInput{
				Bucket:        aws.String(cfg.bucket),
				Key:           aws.String(key),
				Body:          f,
				ContentLength: aws.Int64(fh.Size), // required by some S3-compatible endpoints
			})
			f.Close() // close immediately after upload rather than deferring, to avoid holding handles open across the loop

			if uploadErr != nil {
				log.Printf("S3 upload error for %s: %v", name, uploadErr)
				http.Error(w, "failed to store file", http.StatusInternalServerError)
				return
			}

			// Record this file in the manifest so the download page knows about it
			manifest.Files = append(manifest.Files, transferFile{
				Name: name,
				Size: fh.Size,
				Key:  key,
			})
		}

		// Persist the manifest to S3. This is the last step — if it fails,
		// the transfer is incomplete and should not be accessible.
		if err := saveManifest(r.Context(), cfg, manifest); err != nil {
			log.Printf("manifest save error: %v", err)
			http.Error(w, "failed to save transfer", http.StatusInternalServerError)
			return
		}

		// Send the sender to their new download page.
		// StatusSeeOther (303) tells the browser to follow with a GET request,
		// which prevents a form resubmission if the user hits refresh.
		http.Redirect(w, r, "/d/"+transferID, http.StatusSeeOther)
	}
}

// saveManifest serialises the transfer manifest to JSON and writes it to S3.
// Storing it as manifest.json inside the transfer's own prefix means everything
// for a transfer lives together in S3 — no external database needed.
func saveManifest(ctx context.Context, cfg portalConfig, m transferManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	key := cfg.prefix + m.ID + "/manifest.json"
	contentLen := int64(len(data))
	_, err = cfg.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(cfg.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentType:   aws.String("application/json"),
		ContentLength: aws.Int64(contentLen),
	})
	return err
}
