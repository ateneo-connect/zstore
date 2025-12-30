// Package service provides the core business logic for the erasure coding object storage system.
// This file implements the main FileService for erasure-coded file operations.
//
// FileService provides erasure-coded file operations with the following features:
// - Reed-Solomon erasure coding for fault tolerance
// - Multi-bucket shard distribution via placement strategies
// - Dynamic concurrent downloads with early termination
// - Shard integrity verification using CRC64 hashes
// - Fail-fast upload logic respecting parity shard limits
// - Metadata storage for reconstruction information
//
// Key Operations:
// - UploadFile: Shards file, distributes across buckets, stores metadata
// - DownloadFile: Retrieves shards, verifies integrity, reconstructs file
// - DeleteFile: Removes shards from all buckets and metadata
//
// Architecture:
// - Uses Placer interface for multi-bucket/multi-provider support
// - Integrates with MetadataRepository for shard location tracking
// - Implements dynamic concurrency control for optimal performance
// - Supports configurable Reed-Solomon parameters (data/parity shards)
package service

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/errors"
	"github.com/zzenonn/zstore/internal/placement"
)

type MetadataRepository interface {
	CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	GetMetadata(ctx context.Context, prefix, fileName string) (domain.ObjectMetadata, error)
	ListMetadataByPrefix(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error)
	UpdateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	DeleteMetadata(ctx context.Context, prefix, fileName string) error
}

type FileService struct {
	placer       placement.Placer
	metadataRepo MetadataRepository
	concurrency  int
}

// NewFileService creates a new FileService instance
func NewFileService(placer placement.Placer, metadataRepo MetadataRepository) *FileService {
	return &FileService{
		placer:       placer,
		metadataRepo: metadataRepo,
		concurrency:  1,
	}
}

// UploadFile uploads a file across multiple cloud storage buckets
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int) error {
	start := time.Now()

	// Read file data
	readStart := time.Now()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	log.Debugf("File read took: %v", time.Since(readStart))

	// Check for empty file
	if len(data) == 0 {
		return errors.ErrEmptyFile
	}

	// Create shards using erasure coding
	shardStart := time.Now()
	metadata, shards, err := ShardFile(data, dataShards, parityShards)
	if err != nil {
		return err
	}
	log.Debugf("Sharding took: %v", time.Since(shardStart))

	log.Debugf("Uploading %s", key)

	// Set prefix and filename for metadata
	prefix := filepath.Dir(key)

	metadata.Prefix = prefix
	metadata.FileName = filepath.Base(key)

	// Delete prefix contents if it exists from all buckets
	deleteStart := time.Now()
	buckets := s.placer.ListBuckets()
	for _, bucketName := range buckets {
		if repo, err := s.placer.GetRepositoryForBucket(bucketName); err == nil {
			repo.DeletePrefix(ctx, key) // Ignore errors
		}
	}
	log.Debugf("Delete prefix took: %v", time.Since(deleteStart))

	// Upload shards in parallel
	uploadStart := time.Now()
	if err := s.uploadShards(ctx, key, shards, &metadata, quiet, concurrency, parityShards); err != nil {
		return err
	}
	log.Debugf("Shard uploads took: %v", time.Since(uploadStart))

	// Store metadata
	metadataStart := time.Now()
	_, err = s.metadataRepo.CreateMetadata(ctx, metadata)
	log.Debugf("Metadata storage took: %v", time.Since(metadataStart))
	log.Debugf("Total upload took: %v", time.Since(start))
	return err
}

// DownloadFile downloads a file from cloud storage
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

	// Download shards to temporary files
	shardFiles, err := s.downloadShards(ctx, metadata.ShardHashes, metadata.ParityShards, quiet)
	if err != nil {
		return nil, err
	}

	// Cleanup temp files when done
	defer func() {
		for _, f := range shardFiles {
			if f != nil {
				f.Close()
				os.Remove(f.Name())
			}
		}
	}()

	// Reconstruct file from temp files
	reconstructedData, err := ReconstructFileFromFiles(shardFiles, metadata)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(reconstructedData)), nil
}

// DeleteFile deletes a file from cloud storage
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	// Delete all shards using prefix from all buckets
	log.Debugf("Deleting Key %s", key)
	buckets := s.placer.ListBuckets()
	for _, bucketName := range buckets {
		repo, err := s.placer.GetRepositoryForBucket(bucketName)
		if err != nil {
			continue // Skip failed buckets
		}
		repo.DeletePrefix(ctx, key)
	}

	// Delete metadata
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)
	return s.metadataRepo.DeleteMetadata(ctx, prefix, fileName)
}

// uploadShards uploads erasure-coded shards in parallel with concurrency control
// This function implements the core shard upload strategy:
// 1. Creates goroutines for each shard upload (limited by semaphore)
// 2. Uses fail-fast logic - stops if too many uploads fail
// 3. Updates metadata with actual storage locations after successful uploads
func (s *FileService) uploadShards(ctx context.Context, key string, shards [][]byte, metadata *domain.ObjectMetadata, quiet bool, concurrency, parityShards int) error {
	// Setup channels for goroutine coordination
	var wg sync.WaitGroup
	errorCh := make(chan error, len(shards)) // Buffered to prevent goroutine blocking
	pathCh := make(chan struct {             // Channel for successful upload results
		index       int    // Shard index for metadata update
		storageType string // Storage backend type (e.g., "s3", "gcs")
		bucketName  string // Cloud storage bucket name
		key         string // Actual storage key where shard was stored
	}, len(shards))
	semaphore := make(chan struct{}, concurrency) // Limits concurrent uploads

	// Launch upload goroutines for each shard
	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard []byte) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore slot
			defer func() { <-semaphore }() // Release semaphore slot

			// Generate shard key using original hash from metadata
			// Format: "original-file-key/shard-hash"
			originalHash := metadata.ShardHashes[i].Hash
			shardKey := fmt.Sprintf("%s/%s", key, originalHash)

			// Select bucket and repository for this shard using placement algorithm
			bucketName, repo, err := s.placer.Place(i)
			if err != nil {
				errorCh <- err
				return
			}

			// Upload shard to selected bucket
			path, err := repo.Upload(ctx, shardKey, bytes.NewReader(shard), quiet)
			if err != nil {
				errorCh <- err // Send error to main thread
				return
			}

			// Parse returned path to extract actual storage key
			// Expected format: "bucket/actual-key"
			parts := strings.SplitN(path, "/", 2)
			pathCh <- struct {
				index       int
				storageType string
				bucketName  string
				key         string
			}{
				index:       i,
				storageType: repo.GetStorageType(),
				bucketName:  bucketName,
				key:         parts[1], // Extract key part after bucket
			}
		}(i, shard)
	}

	// Wait for all uploads to complete
	wg.Wait()
	close(errorCh)
	close(pathCh)

	// Implement fail-fast error handling
	// Reed-Solomon can tolerate up to 'parityShards' failures
	// If more than parityShards fail, we cannot guarantee reconstruction
	errorCount := 0
	var uploadErr error
	for err := range errorCh {
		if err != nil {
			errorCount++
			if uploadErr == nil {
				uploadErr = err // Capture first error for reporting
			}
			// Fail fast if too many shards failed
			if errorCount > parityShards {
				return uploadErr
			}
		}
	}
	// If we had some failures but within tolerance, still return error
	if uploadErr != nil {
		return uploadErr
	}

	// Update metadata with actual storage locations
	// This allows the download process to find shards later
	for result := range pathCh {
		metadata.ShardHashes[result.index].StorageType = result.storageType
		metadata.ShardHashes[result.index].BucketName = result.bucketName
		metadata.ShardHashes[result.index].Key = result.key
	}

	return nil
}

// downloadShards downloads shards sequentially to temporary files with robust error handling
func (s *FileService) downloadShards(ctx context.Context, shardHashes []domain.ShardStorage, parityShards int, quiet bool) ([]*os.File, error) {
	// Sequential download strategy with temporary files:
	// 1. Download shards to temp files one by one until we have minShardsNeeded
	// 2. Skip failed shards and continue to next
	// 3. Return temp file handles for reconstruction
	// 4. Caller is responsible for cleanup

	shardFiles := make([]*os.File, len(shardHashes))
	minShardsNeeded := len(shardHashes) - parityShards
	successfulShards := 0
	failedShards := 0

	// Try downloading shards sequentially to temp files
	for i, shardInfo := range shardHashes {
		// Stop early if we have enough shards
		if successfulShards >= minShardsNeeded {
			break
		}

		// Stop if we can't possibly get enough shards
		if failedShards > parityShards {
			break
		}

		// Try to download this shard
		repo, err := s.placer.GetRepositoryForBucket(shardInfo.BucketName)
		if err != nil {
			log.Warnf("Failed to get repository for shard %d: %v", i, err)
			failedShards++
			continue
		}

		reader, err := repo.Download(ctx, shardInfo.Key, quiet)
		if err != nil {
			log.Warnf("Failed to download shard %d: %v", i, err)
			failedShards++
			continue
		}

		// Create temp file for this shard
		tempFile, err := os.CreateTemp("", fmt.Sprintf("shard_%d_*.tmp", i))
		if err != nil {
			log.Warnf("Failed to create temp file for shard %d: %v", i, err)
			reader.Close()
			failedShards++
			continue
		}

		// Stream shard data directly to temp file
		_, err = io.Copy(tempFile, reader)
		reader.Close()
		if err != nil {
			log.Warnf("Failed to write shard %d to temp file: %v", i, err)
			tempFile.Close()
			os.Remove(tempFile.Name())
			failedShards++
			continue
		}

		// Verify shard integrity by reading temp file
		tempFile.Seek(0, 0)
		shardData, err := io.ReadAll(tempFile)
		if err != nil {
			log.Warnf("Failed to read temp file for shard %d: %v", i, err)
			tempFile.Close()
			os.Remove(tempFile.Name())
			failedShards++
			continue
		}

		if err := verifyFileIntegrity(shardData, shardInfo.Hash); err != nil {
			log.Warnf("Shard %d failed integrity check: %v", i, err)
			tempFile.Close()
			os.Remove(tempFile.Name())
			failedShards++
			continue
		}

		// Success - reset file position for reconstruction
		tempFile.Seek(0, 0)
		shardFiles[i] = tempFile
		successfulShards++
		log.Debugf("Successfully downloaded shard %d to temp file (%d/%d needed)", i, successfulShards, minShardsNeeded)
	}

	log.Debugf("%d shards downloaded successfully, %d failed", successfulShards, failedShards)

	if successfulShards < minShardsNeeded {
		// Cleanup on failure
		for _, f := range shardFiles {
			if f != nil {
				f.Close()
				os.Remove(f.Name())
			}
		}
		return nil, errors.ErrInsufficientShards
	}

	return shardFiles, nil
}

// verifyFileIntegrity checks if the reconstructed file matches the expected CRC64 hash
func verifyFileIntegrity(data []byte, expectedHash string) error {
	table := crc64.MakeTable(crc64.ISO)
	fileHash := fmt.Sprintf("%016x", crc64.Checksum(data, table))

	if fileHash != expectedHash {
		log.Debugf("Integrity check failed: expected %s, got %s", expectedHash, fileHash)
		return errors.ErrFileIntegrityCheck
	}
	log.Debugf("File integrity check passed: %s", fileHash)
	return nil
}



// ListFiles lists all files stored under a given prefix
func (s *FileService) ListFiles(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error) {
	return s.metadataRepo.ListMetadataByPrefix(ctx, prefix)
}

// SetConcurrency sets the concurrency limit for uploads
func (s *FileService) SetConcurrency(concurrency int) {
	s.concurrency = concurrency
}
