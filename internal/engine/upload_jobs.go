package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func BuildUploadJobs(root, keyPrefix string) ([]UploadJob, error) {
	var jobs []UploadJob

	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("build relative path for %s: %w", path, err)
		}

		key := filepath.ToSlash(relPath)
		if keyPrefix != "" {
			cleanPrefix := strings.TrimSuffix(keyPrefix, "/")
			key = cleanPrefix + "/" + key
		}

		jobs = append(jobs, UploadJob{
			LocalPath: path,
			Key:       key,
			Size:      info.Size(),
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk folder %s: %w", root, err)
	}

	return jobs, nil
}
