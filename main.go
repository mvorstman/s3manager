package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func main() {
	endpoint := flag.String("endpoint", "", "Base URL, for example https://example.com")
	method := flag.String("method", "GET", "HTTP method")
	path := flag.String("path", "/", "Request path")
	query := flag.String("query", "", "Query string")
	region := flag.String("region", "us-east-1", "AWS region for signing")

	flag.Parse()

	fmt.Println("S3Manager starting...")

	if *endpoint == "" {
		fmt.Println("ERROR: endpoint is required")
		return
	}

	sendRequest(*endpoint, *method, *path, *query, *region)
}

func sendRequest(endpoint, method, path, query, region string) {
	url := endpoint + path
	if query != "" {
		url = url + "?" + query
	}

	fmt.Println("Preparing request...")
	fmt.Println("Method:", method)
	fmt.Println("Endpoint:", endpoint)
	fmt.Println("Path:", path)
	fmt.Println("Query:", query)
	fmt.Println("Full URL:", url)

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		fmt.Println("Failed to create request:", err)
		return
	}

	req.Header.Set("User-Agent", "S3Manager-Go-Learning/0.1")

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	req.Header.Set("x-amz-date", amzDate)

	emptyHash := sha256.Sum256([]byte(""))
	payloadHash := fmt.Sprintf("%x", emptyHash)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	fmt.Println("Outgoing headers:")
	for name, values := range req.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", name, value)
		}
	}

	canonicalRequest, signedHeaders := buildCanonicalRequest(req, path, query, payloadHash)

	fmt.Println("Signed headers:", signedHeaders)
	fmt.Println("Canonical request:")
	fmt.Println("-----BEGIN CANONICAL REQUEST-----")
	fmt.Println(canonicalRequest)
	fmt.Println("-----END CANONICAL REQUEST-----")

	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))
	canonicalRequestHashHex := fmt.Sprintf("%x", canonicalRequestHash)

	fmt.Println("Canonical request SHA256:")
	fmt.Println(canonicalRequestHashHex)

	dateStamp := amzDate[:8]
	credentialScope := dateStamp + "/" + region + "/s3/aws4_request"

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		canonicalRequestHashHex,
	}, "\n")

	fmt.Println("Credential scope:")
	fmt.Println(credentialScope)

	fmt.Println("String to sign:")
	fmt.Println("-----BEGIN STRING TO SIGN-----")
	fmt.Println(stringToSign)
	fmt.Println("-----END STRING TO SIGN-----")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	fmt.Println("Sending request...")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("HTTP request failed:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Status:", resp.Status)

	fmt.Println("Response headers:")
	for name, values := range resp.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", name, value)
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Failed to read response body:", err)
		return
	}

	fmt.Println("Response body:")
	fmt.Println(string(body))
}

func buildCanonicalRequest(req *http.Request, path, query, payloadHash string) (string, string) {
	var headerNames []string
	canonicalHeaders := make(map[string]string)

	hostValue := req.URL.Host
	headerNames = append(headerNames, "host")
	canonicalHeaders["host"] = hostValue

	for name, values := range req.Header {
		lowerName := strings.ToLower(strings.TrimSpace(name))
		value := strings.Join(values, ",")
		value = strings.TrimSpace(value)

		headerNames = append(headerNames, lowerName)
		canonicalHeaders[lowerName] = value
	}

	sort.Strings(headerNames)

	var canonicalHeadersText strings.Builder
	for _, name := range headerNames {
		canonicalHeadersText.WriteString(name)
		canonicalHeadersText.WriteString(":")
		canonicalHeadersText.WriteString(canonicalHeaders[name])
		canonicalHeadersText.WriteString("\n")
	}

	signedHeaders := strings.Join(headerNames, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		path,
		query,
		canonicalHeadersText.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	return canonicalRequest, signedHeaders
}