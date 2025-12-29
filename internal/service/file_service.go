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

	// Download shards using dynamic concurrency strategy
	shards, err := s.downloadShards(ctx, metadata.ShardHashes, metadata.ParityShards, quiet)
	if err != nil {
		return nil, err
	}

	// Phase 5: Reconstruct original file from available shards
	// Reed-Solomon can reconstruct with any minShardsNeeded valid shards
	reconstructedData, err := ReconstructFile(shards, metadata)
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

// uploadShards uploads erasure-coded shards using optimized worker pool pattern
func (s *FileService) uploadShards(ctx context.Context, key string, shards [][]byte, metadata *domain.ObjectMetadata, quiet bool, concurrency, parityShards int) error {
	// Pre-allocate channels with exact capacity to reduce overhead
	jobCh := make(chan int, len(shards)) // Just send shard indices
	resultCh := make(chan struct {
		index       int
		storageType string
		bucketName  string
		key         string
		err         error
	}, len(shards))
	
	// Start workers immediately
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shardIndex := range jobCh {
				shard := shards[shardIndex]
				
				// Generate shard key
				originalHash := metadata.ShardHashes[shardIndex].Hash
				shardKey := fmt.Sprintf("%s/%s", key, originalHash)
				
				// Select bucket and repository
				bucketName, repo, err := s.placer.Place(shardIndex)
				if err != nil {
					resultCh <- struct {
						index       int
						storageType string
						bucketName  string
						key         string
						err         error
					}{index: shardIndex, err: err}
					continue
				}
				
				// Upload shard
				path, err := repo.Upload(ctx, shardKey, bytes.NewReader(shard), quiet)
				if err != nil {
					resultCh <- struct {
						index       int
						storageType string
						bucketName  string
						key         string
						err         error
					}{index: shardIndex, err: err}
					continue
				}
				
				// Parse path and send success result
				parts := strings.SplitN(path, "/", 2)
				resultCh <- struct {
					index       int
					storageType string
					bucketName  string
					key         string
					err         error
				}{
					index:       shardIndex,
					storageType: repo.GetStorageType(),
					bucketName:  bucketName,
					key:         parts[1],
				}
			}
		}()
	}
	
	// Send all jobs to workers
	for i := range shards {
		jobCh <- i
	}
	close(jobCh)
	
	// Wait for workers and close result channel
	go func() {
		wg.Wait()
		close(resultCh)
	}()
	
	// Collect results
	errorCount := 0
	var uploadErr error
	for result := range resultCh {
		if result.err != nil {
			errorCount++
			if uploadErr == nil {
				uploadErr = result.err
			}
			if errorCount > parityShards {
				return uploadErr
			}
		} else {
			// Update metadata
			metadata.ShardHashes[result.index].StorageType = result.storageType
			metadata.ShardHashes[result.index].BucketName = result.bucketName
			metadata.ShardHashes[result.index].Key = result.key
		}
	}
	
	if uploadErr != nil {
		return uploadErr
	}
	return nil
}

// downloadShards downloads shards using optimized concurrency pattern
func (s *FileService) downloadShards(ctx context.Context, shardHashes []domain.ShardStorage, parityShards int, quiet bool) ([][]byte, error) {
	// Calculate minimum shards needed
	minShardsNeeded := len(shardHashes) - parityShards
	shards := make([][]byte, len(shardHashes))
	
	// Use semaphore for concurrency control
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successfulShards := 0
	
	// Context for early termination
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	// Launch downloads with immediate semaphore acquisition
	for i, shardInfo := range shardHashes {
		wg.Add(1)
		go func(i int, shardInfo domain.ShardStorage) {
			defer wg.Done()
			
			// Check if we should continue
			select {
			case <-ctx.Done():
				return
			default:
			}
			
			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()
			
			// Download shard
			repo, err := s.placer.GetRepositoryForBucket(shardInfo.BucketName)
			if err != nil {
				return
			}
			
			reader, err := repo.Download(ctx, shardInfo.Key, quiet)
			if err != nil {
				return
			}
			
			shardData, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				return
			}
			
			// Verify integrity
			if err := verifyFileIntegrity(shardData, shardInfo.Hash); err != nil {
				log.Warnf("Shard %d failed integrity check", i)
				return
			}
			
			// Update results with mutex
			mu.Lock()
			shards[i] = shardData
			successfulShards++
			
			// Early termination if we have enough
			if successfulShards >= minShardsNeeded {
				cancel()
			}
			mu.Unlock()
		}(i, shardInfo)
	}
	
	// Wait for completion
	wg.Wait()
	
	log.Debugf("%d shards downloaded successfully", successfulShards)
	
	if successfulShards < minShardsNeeded {
		return nil, errors.ErrInsufficientShards
	}
	
	return shards, nil
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
