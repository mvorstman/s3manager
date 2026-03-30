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

func handleDownloadPage(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		m, err := loadManifest(r.Context(), cfg, id)
		if err != nil {
			log.Printf("load manifest error for %q: %v", id, err)
			renderNotFoundPage(w)
			return
		}
		if time.Now().UTC().After(m.ExpiresAt) {
			renderExpiredPage(w, m)
			return
		}
		renderDownloadPage(w, m)
	}
}

func handleDownloadFile(cfg portalConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		filename := r.PathValue("filename")

		m, err := loadManifest(r.Context(), cfg, id)
		if err != nil {
			http.Error(w, "transfer not found", http.StatusNotFound)
			return
		}
		if time.Now().UTC().After(m.ExpiresAt) {
			http.Error(w, "transfer has expired", http.StatusGone)
			return
		}

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

		w.Header().Set("Content-Disposition",
			fmt.Sprintf(`attachment; filename*=UTF-8''%s`, url.PathEscape(filename)))
		w.Header().Set("Content-Type", "application/octet-stream")
		if out.ContentLength != nil {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", *out.ContentLength))
		}

		if _, err := io.Copy(w, out.Body); err != nil {
			log.Printf("stream error for %q: %v", fileKey, err)
		}
	}
}
