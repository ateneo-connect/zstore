package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/errors"
)

func init() {
	// Set log level based on environment variables
	switch logLevel := strings.ToLower(os.Getenv("LOG_LEVEL")); logLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}
}

type S3ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader, quiet bool) (string, error)
	Download(ctx context.Context, key string, quiet bool) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
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

// UploadFileRaw uploads a file directly to S3 without erasure coding
func (s *FileService) UploadFileRaw(ctx context.Context, key string, r io.Reader, quiet bool) error {
	log.Debugf("Uploading raw file %s", key)
	_, err := s.objectRepo.Upload(ctx, key, r, quiet)
	return err
}

// DownloadFileRaw downloads a file directly from S3 without erasure coding reconstruction
func (s *FileService) DownloadFileRaw(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	log.Debugf("Downloading raw file %s", key)
	return s.objectRepo.Download(ctx, key, quiet)
}

// UploadFile uploads a file to S3
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards int) error {

	// Read file data
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// Check for empty file
	if len(data) == 0 {
		return errors.ErrEmptyFile
	}

	// Create shards using erasure coding
	metadata, shards, err := ShardFile(data, dataShards, parityShards)
	if err != nil {
		return err
	}

	log.Debugf("Uploading %s", key)

	// Set prefix and filename for metadata
	prefix := filepath.Dir(key)

	metadata.Prefix = prefix
	metadata.FileName = filepath.Base(key)

	// Delete prefix contents if it exists
	s.objectRepo.DeletePrefix(ctx, key)

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
func (s *FileService) DownloadFile(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	// Get prefix and filename for metadata lookup
	prefix := filepath.Dir(key)

	fileName := filepath.Base(key)

	// Get metadata
	metadata, err := s.metadataRepo.GetMetadata(ctx, prefix, fileName)
	if err != nil {
		return nil, err
	}

	log.Debugf("Object Metadata: %+v\n", metadata)

	// Download each shard in parallel
	shards := make([][]byte, len(metadata.ShardHashes))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, s.concurrency)

	for i, shardInfo := range metadata.ShardHashes {
		wg.Add(1)
		go func(i int, shardInfo domain.ShardStorage) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			reader, err := s.objectRepo.Download(ctx, shardInfo.Key, quiet)
			if err != nil {
				// Use empty byte array for missing shards
				shards[i] = nil
				return
			}
			defer reader.Close()

			shardData, err := io.ReadAll(reader)
			if err != nil {
				// Use empty byte array for corrupted shards
				shards[i] = nil
				return
			}

			shards[i] = shardData
		}(i, shardInfo)
	}

	wg.Wait()

	// Count nil shards and check if we have enough for reconstruction
	nilCount := 0
	for _, shard := range shards {
		if shard == nil {
			nilCount++
		}
	}
	log.Debugf("%d shards missing", nilCount)

	if nilCount > metadata.ParityShards {
		return nil, errors.ErrInsufficientShards
	}

	// Reassemble the file
	reconstructedData, err := ReconstructFile(shards, metadata)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(reconstructedData)), nil
}

// DeleteFile deletes a file from S3
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	// Delete all shards using prefix
	log.Debugf("Deleting Key %s", key)
	if err := s.objectRepo.DeletePrefix(ctx, key); err != nil {
		return err
	}

	// Delete metadata
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)
	return s.metadataRepo.DeleteMetadata(ctx, prefix, fileName)
}

// SetConcurrency sets the concurrency limit for uploads
func (s *FileService) SetConcurrency(concurrency int) {
	s.concurrency = concurrency
}
