package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zzenonn/zstore/internal/domain"
)

type S3ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader, quiet bool) (string, error)
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	GetBucketName() string
	GetStorageType() string
}

type MetadataRepository interface {
	CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	GetMetadata(ctx context.Context, prefix, fileName string) (domain.ObjectMetadata, error)
	ListMetadataByPrefix(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error)
	UpdateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	DeleteMetadata(ctx context.Context, prefix, fileName string) error
}

type FileService struct {
	objectRepo   S3ObjectRepository
	metadataRepo MetadataRepository
	concurrency  int
}

// NewFileService creates a new FileService instance
func NewFileService(objectRepo S3ObjectRepository, metadataRepo MetadataRepository) *FileService {
	return &FileService{
		objectRepo:   objectRepo,
		metadataRepo: metadataRepo,
		concurrency:  3, // Default concurrency limit
	}
}

// UploadFile uploads a file to S3
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader, quiet bool) error {

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

	// Upload each shard in parallel with concurrency limit
	var wg sync.WaitGroup
	errorCh := make(chan error, len(shards))
	pathCh := make(chan struct {
		index       int
		storageType string
		bucketName  string
		key         string
	}, len(shards))
	semaphore := make(chan struct{}, s.concurrency) // Limit concurrent uploads

	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard []byte) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			// Use original hash as part of shard key
			originalHash := metadata.ShardHashes[i].Hash
			shardKey := fmt.Sprintf("%s/%s", key, originalHash)
			if path, err := s.objectRepo.Upload(ctx, shardKey, bytes.NewReader(shard), quiet); err != nil {
				errorCh <- err
			} else {
				// Parse key from path (format: bucket/key)
				parts := strings.SplitN(path, "/", 2)
				pathCh <- struct {
					index       int
					storageType string
					bucketName  string
					key         string
				}{
					index:       i,
					storageType: s.objectRepo.GetStorageType(),
					bucketName:  s.objectRepo.GetBucketName(),
					key:         parts[1],
				}
			}
		}(i, shard)
	}

	wg.Wait()
	close(errorCh)
	close(pathCh)

	if err := <-errorCh; err != nil {
		return err
	}

	// Update ShardHashes with storage metadata
	for result := range pathCh {
		// Update storage info while preserving hash
		metadata.ShardHashes[result.index].StorageType = result.storageType
		metadata.ShardHashes[result.index].BucketName = result.bucketName
		metadata.ShardHashes[result.index].Key = result.key
	}

	// Store metadata
	_, err = s.metadataRepo.CreateMetadata(ctx, metadata)
	return err
}

// DownloadFile downloads a file from S3
func (s *FileService) DownloadFile(ctx context.Context, key string) (io.ReadCloser, error) {
	// Get prefix and filename for metadata lookup
	prefix := filepath.Dir(key)
	if prefix == "." {
		prefix = "root"
	}
	fileName := filepath.Base(key)
	
	// Get metadata
	metadata, err := s.metadataRepo.GetMetadata(ctx, prefix, fileName)
	if err != nil {
		return nil, err
	}
	
	fmt.Printf("Metadata: %+v\n", metadata)
	
	// TODO: Implement shard reassembly using metadata.ShardHashes
	_ = metadata
	return s.objectRepo.Download(ctx, key)
}

// DeleteFile deletes a file from S3
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	return s.objectRepo.Delete(ctx, key)
}
