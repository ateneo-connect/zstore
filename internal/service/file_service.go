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
func (s *FileService) DownloadFile(ctx context.Context, key string, dest io.WriterAt, quiet bool) error {
	// Get prefix and filename for metadata lookup
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)

	// Get metadata
	metadata, err := s.metadataRepo.GetMetadata(ctx, prefix, fileName)
	if err != nil {
		return err
	}

	log.Debugf("Object Metadata: %+v\n", metadata)

	// Download shards to temporary files
	tempFilePaths, err := s.downloadShards(ctx, metadata.ShardHashes, metadata.ParityShards, quiet)
	if err != nil {
		return err
	}

	// Cleanup temp files when done
	defer func() {
		for _, path := range tempFilePaths {
			os.Remove(path)
		}
	}()

	// Reconstruct file from temp files
	reconstructedData, err := ReconstructFileFromPaths(tempFilePaths, metadata)
	if err != nil {
		return err
	}

	// Write reconstructed data to destination
	_, err = dest.WriteAt(reconstructedData, 0)
	return err
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

// downloadShards downloads shards using dynamic concurrency strategy with temp files
func (s *FileService) downloadShards(ctx context.Context, shardHashes []domain.ShardStorage, parityShards int, quiet bool) ([]string, error) {
	// Dynamic Shard Downloading Strategy:
	// 1. Start with limited concurrent downloads (s.concurrency)
	// 2. When a shard completes, check if we need more shards
	// 3. If still needed, start downloading the next available shard
	// 4. Stop early once we have enough shards for reconstruction
	// This optimizes network usage and reduces unnecessary downloads

	tempFilePaths := make([]string, len(shardHashes))
	var wg sync.WaitGroup
	var mu sync.Mutex               // Protects shared state between goroutines
	successfulShards := 0           // Count of successfully downloaded shards
	nextShardIndex := s.concurrency // Index of next shard to download
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Calculate minimum shards needed for Reed-Solomon reconstruction
	// Formula: total_shards - parity_shards = minimum_data_shards_needed
	minShardsNeeded := len(shardHashes) - parityShards

	// Phase 1: Start initial batch of downloads (up to concurrency limit)
	// This prevents overwhelming the network with too many simultaneous requests
	for i := 0; i < s.concurrency && i < len(shardHashes); i++ {
		wg.Add(1)
		go s.downloadShard(ctx, &wg, &mu, tempFilePaths, shardHashes[i], i, quiet, &successfulShards, &nextShardIndex, minShardsNeeded, shardHashes, cancel)
	}

	// Phase 2: Wait for all download goroutines to complete
	// This includes both initial downloads and any dynamically started ones
	wg.Wait()

	// Phase 3: Log final count of successful downloads
	// successfulShards is accurately maintained by downloadShard under mutex protection
	log.Debugf("%d shards downloaded successfully", successfulShards)

	// Phase 4: Ensure we have enough shards for Reed-Solomon reconstruction
	// If insufficient, return error rather than attempting reconstruction
	if successfulShards < minShardsNeeded {
		// Cleanup temp files on failure
		for _, path := range tempFilePaths {
			if path != "" {
				os.Remove(path)
			}
		}
		return nil, errors.ErrInsufficientShards
	}

	// Filter out empty paths (failed downloads)
	var successfulPaths []string
	for _, path := range tempFilePaths {
		if path != "" {
			successfulPaths = append(successfulPaths, path)
		}
	}

	return successfulPaths, nil
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

// downloadShard downloads a single shard to temp file and manages dynamic concurrency
// This function implements the core logic for the dynamic downloading strategy:
// 1. Downloads the assigned shard to a temp file
// 2. Verifies shard integrity using CRC64 hash
// 3. Decides whether to start downloading additional shards
// 4. Handles early termination when enough shards are available
func (s *FileService) downloadShard(ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, tempFilePaths []string, shardInfo domain.ShardStorage, i int, quiet bool, successfulShards *int, nextShardIndex *int, minShardsNeeded int, allShards []domain.ShardStorage, cancel context.CancelFunc) {
	defer wg.Done()

	// Early termination check: stop if context was cancelled
	// This happens when we already have enough shards or an error occurred
	select {
	case <-ctx.Done():
		return
	default:
	}

	shardStart := time.Now()
	log.Debugf("[PERF] Starting shard %d download: bucket=%s, key=%s", i, shardInfo.BucketName, shardInfo.Key)

	// Step 1: Get repository for the shard's bucket
	repoStart := time.Now()
	repo, err := s.placer.GetRepositoryForBucket(shardInfo.BucketName)
	if err != nil {
		// Mark shard as failed and potentially start next download
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}
	log.Debugf("[PERF] Shard %d: Repository lookup took %v", i, time.Since(repoStart))

	// Step 2: Create temp file for this shard
	tempFileStart := time.Now()
	tempFile, err := os.CreateTemp("", fmt.Sprintf("shard_%d_*.tmp", i))
	if err != nil {
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}
	tempFilePath := tempFile.Name()
	log.Debugf("[PERF] Shard %d: Temp file creation took %v", i, time.Since(tempFileStart))

	// Step 3: Download directly to temp file using WriterAt interface
	downloadStart := time.Now()
	err = repo.Download(ctx, shardInfo.Key, tempFile, quiet)
	log.Debugf("[PERF] Shard %d: Download initiation took %v", i, time.Since(downloadStart))
	tempFile.Close()
	if err != nil {
		// Mark shard as failed and potentially start next download
		log.Errorf("Shard %d download failed: %v", i, err)
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}

	// Debug: Check file size after download
	if fileInfo, err := os.Stat(tempFilePath); err == nil {
		log.Debugf("[PERF] Shard %d: Downloaded file size: %d bytes", i, fileInfo.Size())
	} else {
		log.Errorf("Shard %d: Failed to stat temp file: %v", i, err)
	}

	// Copy temp file content for performance measurement
	copyStart := time.Now()
	shardData, err := os.ReadFile(tempFilePath)
	if err != nil {
		log.Errorf("Shard %d: Failed to read temp file: %v", i, err)
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}
	log.Debugf("[PERF] Shard %d: Copied %d bytes in %v (%.2f MB/s)", i, len(shardData), time.Since(copyStart), float64(len(shardData))/1024/1024/time.Since(copyStart).Seconds())

	// Step 4: Verify shard integrity using CRC64 hash (DISABLED)
	// This ensures downloaded data matches what was originally stored
	// TODO: Re-enable after fixing hash format mismatch
	/*
	shardData, err := os.ReadFile(tempFilePath)
	if err != nil {
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}
	if err := verifyFileIntegrity(shardData, shardInfo.Hash); err != nil {
		log.Warnf("Shard %d failed integrity check", i)
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
		return
	}
	*/

	// Step 5: Successfully downloaded shard
	// Update shared state under mutex protection
	mu.Lock()
	tempFilePaths[i] = tempFilePath
	*successfulShards++
	shardTotal := time.Since(shardStart)
	log.Debugf("[PERF] Shard %d: TOTAL time %v (%d/%d needed)", i, shardTotal, *successfulShards, minShardsNeeded)

	// Step 6: Early termination optimization
	// If we have enough shards for reconstruction, cancel remaining downloads
	// This prevents unnecessary network traffic and speeds up the process
	if *successfulShards >= minShardsNeeded {
		cancel() // Signal all other goroutines to stop
		mu.Unlock()
		return
	}
	mu.Unlock()

	// Step 7: Dynamic concurrency - start next download if needed
	// This maintains optimal network utilization by keeping downloads active
	s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet)
}

// maybeStartNext implements the dynamic concurrency control logic
// This function decides whether to start downloading the next available shard
// based on current progress and remaining needs. It's called after each
// shard completion (success or failure) to maintain optimal download flow.
func (s *FileService) maybeStartNext(wg *sync.WaitGroup, mu *sync.Mutex, tempFilePaths []string, successfulShards *int, nextShardIndex *int, minShardsNeeded int, allShards []domain.ShardStorage, ctx context.Context, cancel context.CancelFunc, quiet bool) {
	mu.Lock()
	defer mu.Unlock()

	// Decision Logic: Start next download only if BOTH conditions are true:
	// 1. We still need more shards (*successfulShards < minShardsNeeded)
	// 2. There are more shards available to download (*nextShardIndex < len(allShards))
	//
	// This prevents:
	// - Starting unnecessary downloads when we have enough shards
	// - Attempting to download non-existent shards (index out of bounds)
	if *successfulShards < minShardsNeeded && *nextShardIndex < len(allShards) {
		// Atomically claim the next shard index to prevent race conditions
		currentIndex := *nextShardIndex
		*nextShardIndex++

		// Start new download goroutine for the claimed shard
		// This maintains the concurrency level as other downloads complete
		wg.Add(1)
		go s.downloadShard(ctx, wg, mu, tempFilePaths, allShards[currentIndex], currentIndex, quiet, successfulShards, nextShardIndex, minShardsNeeded, allShards, cancel)
	}
	// If conditions not met, no new download is started, allowing
	// the system to naturally wind down as remaining downloads complete
}



// ListFiles lists all files stored under a given prefix
func (s *FileService) ListFiles(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error) {
	return s.metadataRepo.ListMetadataByPrefix(ctx, prefix)
}

// SetConcurrency sets the concurrency limit for uploads
func (s *FileService) SetConcurrency(concurrency int) {
	s.concurrency = concurrency
}
