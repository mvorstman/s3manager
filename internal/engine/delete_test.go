package engine

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDeletePrefixRunsBatchesConcurrently(t *testing.T) {
	t.Parallel()

	var inFlight int64
	var maxInFlight int64
	var deleteCalls int64

	client := newTestS3Client(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			return xmlResponse(http.StatusOK, listObjectsV2Response(
				"prefix/object-1",
				"prefix/object-2",
				"prefix/object-3",
				"prefix/object-4",
			)), nil

		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			current := atomic.AddInt64(&inFlight, 1)
			updateAtomicMax(&maxInFlight, current)
			atomic.AddInt64(&deleteCalls, 1)

			time.Sleep(100 * time.Millisecond)

			atomic.AddInt64(&inFlight, -1)
			return xmlResponse(http.StatusOK, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`), nil

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
			return nil, nil
		}
	})

	result, err := DeletePrefix(
		context.Background(),
		client,
		"bucket",
		"prefix/",
		1000,
		false,
		4,
		false,
		false,
		1,
	)
	if err != nil {
		t.Fatalf("delete prefix: %v", err)
	}

	if got := atomic.LoadInt64(&deleteCalls); got != 4 {
		t.Fatalf("delete calls = %d, want 4", got)
	}
	if got := atomic.LoadInt64(&maxInFlight); got < 2 {
		t.Fatalf("max concurrent delete calls = %d, want at least 2", got)
	}
	if result.BatchCalls != 4 {
		t.Fatalf("batch calls = %d, want 4", result.BatchCalls)
	}
	if result.Deleted != 4 {
		t.Fatalf("deleted = %d, want 4", result.Deleted)
	}
}

func TestDeletePrefixDryRunDoesNotDelete(t *testing.T) {
	t.Parallel()

	var deleteCalls int64

	client := newTestS3Client(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			return xmlResponse(http.StatusOK, listObjectsV2Response("prefix/object-1", "prefix/object-2")), nil

		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			atomic.AddInt64(&deleteCalls, 1)
			return xmlResponse(http.StatusInternalServerError, ""), nil

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
			return nil, nil
		}
	})

	result, err := DeletePrefix(
		context.Background(),
		client,
		"bucket",
		"prefix/",
		1000,
		true,
		4,
		false,
		false,
		1,
	)
	if err != nil {
		t.Fatalf("dry-run delete prefix: %v", err)
	}

	if got := atomic.LoadInt64(&deleteCalls); got != 0 {
		t.Fatalf("delete calls = %d, want 0", got)
	}
	if result.Queued != 2 {
		t.Fatalf("queued = %d, want 2", result.Queued)
	}
	if result.Deleted != 0 {
		t.Fatalf("deleted = %d, want 0", result.Deleted)
	}
}

type doFunc func(*http.Request) (*http.Response, error)

func (fn doFunc) Do(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func newTestS3Client(fn doFunc) *awss3.Client {
	return awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", "")),
		HTTPClient:  fn,
	}, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.test")
	})
}

func xmlResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/xml"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func listObjectsV2Response(keys ...string) string {
	type content struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Xmlns       string    `xml:"xmlns,attr"`
		IsTruncated bool      `xml:"IsTruncated"`
		Contents    []content `xml:"Contents"`
	}

	contents := make([]content, 0, len(keys))
	for _, key := range keys {
		contents = append(contents, content{Key: key, Size: 1024})
	}

	out, err := xml.Marshal(response{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		IsTruncated: false,
		Contents:    contents,
	})
	if err != nil {
		panic(err)
	}

	return xml.Header + string(out)
}
