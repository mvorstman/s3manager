package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// portalConfig holds all the settings the Transfer Portal needs to operate.
// It's created once at startup and passed into every HTTP handler.
type portalConfig struct {
	client        *s3.Client // the S3 client used for all storage operations
	bucket        string     // which S3 bucket to store transfers in
	prefix        string     // key prefix for all transfers, e.g. "transfers/"
	expiryHours   int        // how many hours a transfer link stays valid
	maxUploadSize int64      // maximum allowed upload size in bytes
}

// startPortal registers all HTTP routes and starts the web server.
// It blocks until the server exits (which in normal operation means forever).
//
// Routes:
//   GET  /              → upload page (the drag-and-drop UI)
//   POST /upload        → receives the uploaded files and creates the transfer
//   GET  /d/{id}        → download page for a specific transfer
//   GET  /d/{id}/{file} → streams a single file from S3 to the browser
func startPortal(ctx context.Context, cfg portalConfig, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", handleUploadPage(cfg))
	mux.HandleFunc("POST /upload", handleUpload(cfg))
	mux.HandleFunc("GET /d/{id}", handleDownloadPage(cfg))
	mux.HandleFunc("GET /d/{id}/{filename}", handleDownloadFile(cfg))

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Transfer Portal listening on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
