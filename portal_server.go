package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type portalConfig struct {
	client        *s3.Client
	bucket        string
	prefix        string
	expiryHours   int
	maxUploadSize int64
}

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
