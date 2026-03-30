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

type transferManifest struct {
	ID        string         `json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	ExpiresAt time.Time      `json:"expires_at"`
	Message   string         `json:"message,omitempty"`
	Files     []transferFile `json:"files"`
}

type transferFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Key  string `json:"key"`
}

func newTransferID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func handleUploadPage(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderUploadPage(w, uploadPageData{ExpiryHours: cfg.expiryHours})
	}
}

func handleUpload(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, cfg.maxUploadSize)
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			renderUploadPage(w, uploadPageData{
				ExpiryHours: cfg.expiryHours,
				Error:       "Upload exceeds size limit or is malformed.",
			})
			return
		}

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

		message := strings.TrimSpace(r.FormValue("message"))
		now := time.Now().UTC()
		manifest := transferManifest{
			ID:        transferID,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Duration(cfg.expiryHours) * time.Hour),
			Message:   message,
		}

		for _, fh := range files {
			name := fh.Filename
			if name == "manifest.json" || strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
				renderUploadPage(w, uploadPageData{
					ExpiryHours: cfg.expiryHours,
					Error:       fmt.Sprintf("Invalid filename: %q", name),
				})
				return
			}

			f, err := fh.Open()
			if err != nil {
				http.Error(w, "failed to read uploaded file", http.StatusInternalServerError)
				return
			}

			key := cfg.prefix + transferID + "/" + name
			_, uploadErr := cfg.client.PutObject(r.Context(), &s3.PutObjectInput{
				Bucket:        aws.String(cfg.bucket),
				Key:           aws.String(key),
				Body:          f,
				ContentLength: aws.Int64(fh.Size),
			})
			f.Close()

			if uploadErr != nil {
				log.Printf("S3 upload error for %s: %v", name, uploadErr)
				http.Error(w, "failed to store file", http.StatusInternalServerError)
				return
			}

			manifest.Files = append(manifest.Files, transferFile{
				Name: name,
				Size: fh.Size,
				Key:  key,
			})
		}

		if err := saveManifest(r.Context(), cfg, manifest); err != nil {
			log.Printf("manifest save error: %v", err)
			http.Error(w, "failed to save transfer", http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, "/d/"+transferID, http.StatusSeeOther)
	}
}

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
