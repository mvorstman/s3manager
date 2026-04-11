package engine

import "time"

type UploadJob struct {
	LocalPath string
	Key       string
	Size      int64
}

type UploadFileResult struct {
	LocalPath string
	Key       string
	Bytes     int64
	Duration  time.Duration
	Err       error
}

type UploadFolderResult struct {
	TotalFiles      int
	SuccessfulFiles int
	FailedFiles     int
	TotalBytes      int64
	UploadedBytes   int64
	FailedBytes     int64
	Duration        time.Duration
	Files           []UploadFileResult
}
