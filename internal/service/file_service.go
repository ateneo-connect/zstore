package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/zzenonn/zstore/internal/domain"
)

type S3ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

type MetadataRepository interface {
	CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
}

type FileService struct {
	repo         S3ObjectRepository
	metadataRepo MetadataRepository
}

// NewFileService creates a new FileService instance
func NewFileService(repo S3ObjectRepository, metadataRepo MetadataRepository) *FileService {
	return &FileService{
		repo:         repo,
		metadataRepo: metadataRepo,
	}
}

// UploadFile uploads a file to S3
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader) error {

	// Read file data
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// Create shards using erasure coding
	metadata, shards, err := ShardFile(data, 4, 2) // 2 data shards, 2 parity shards
	if err != nil {
		return err
	}

	// Set prefix and filename for metadata
	prefix := filepath.Dir(key)
	if prefix == "." {
		prefix = "root"
	}
	metadata.Prefix = prefix
	metadata.FileName = filepath.Base(key)

	// Upload each shard in parallel
	var wg sync.WaitGroup
	errorCh := make(chan error, len(shards))

	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard []byte) {
			defer wg.Done()
			shardKey := fmt.Sprintf("%s.shard_%d", key, i)
			if err := s.repo.Upload(ctx, shardKey, bytes.NewReader(shard)); err != nil {
				errorCh <- err
			}
		}(i, shard)
	}

	wg.Wait()
	close(errorCh)

	if err := <-errorCh; err != nil {
		return err
	}

	// Store metadata
	_, err = s.metadataRepo.CreateMetadata(ctx, metadata)
	return err
}

// DownloadFile downloads a file from S3
func (s *FileService) DownloadFile(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.repo.Download(ctx, key)
}

// DeleteFile deletes a file from S3
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	return s.repo.Delete(ctx, key)
}
