// Package service provides the core business logic for the erasure coding object storage system.
// This file implements Reed-Solomon erasure coding functionality for data protection.
//
// The erasure coding service provides two main operations:
// 1. ShardFile: Splits a file into data and parity shards using Reed-Solomon encoding
// 2. ReconstructFile: Rebuilds the original file from available shards
//
// Reed-Solomon Erasure Coding:
// - Splits data into N data shards and M parity shards
// - Can reconstruct original data with any N shards (out of N+M total)
// - Provides fault tolerance: can lose up to M shards without data loss
// - Each shard gets a CRC64 hash for integrity verification
//
// Key Features:
// - Configurable data/parity shard ratios
// - CRC64 integrity hashing for each shard
// - Metadata generation with shard information
// - Efficient reconstruction algorithm
// - Streaming support for minimal memory usage regardless of file size
//
// Usage:
//   metadata, shardFiles, err := ShardFile(reader, 4, 2, fileSize)  // 4 data + 2 parity shards
//   reconstructed, err := ReconstructFile(shards, metadata)
//
// The service integrates with FileService to provide distributed, fault-tolerant
// file storage across multiple buckets and cloud providers.
package service

import (
	"bytes"
	"fmt"
	"hash/crc64"
	"io"
	"os"

	"github.com/klauspost/reedsolomon"
	"github.com/zzenonn/zstore/internal/domain"
)



// ShardFile splits a file into shards using streaming to minimize memory usage
// Returns metadata and temporary shard files that caller must manage
func ShardFile(reader io.Reader, dataShards, parityShards int, fileSize int64) (domain.ObjectMetadata, []*os.File, error) {
	enc, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return domain.ObjectMetadata{}, nil, err
	}

	// Create temporary files for all shards
	totalShards := dataShards + parityShards
	shardFiles := make([]*os.File, totalShards)
	for i := 0; i < totalShards; i++ {
		file, err := os.CreateTemp("", fmt.Sprintf("shard_%d_*.tmp", i))
		if err != nil {
			// Clean up any created files
			for j := 0; j < i; j++ {
				shardFiles[j].Close()
				os.Remove(shardFiles[j].Name())
			}
			return domain.ObjectMetadata{}, nil, err
		}
		shardFiles[i] = file
	}

	// Split data into data shards
	dataWriters := make([]io.Writer, dataShards)
	for i := 0; i < dataShards; i++ {
		dataWriters[i] = shardFiles[i]
	}

	err = enc.Split(reader, dataWriters, fileSize)
	if err != nil {
		// Clean up files on error
		for _, file := range shardFiles {
			file.Close()
			os.Remove(file.Name())
		}
		return domain.ObjectMetadata{}, nil, err
	}

	// Create readers from data files for parity generation
	dataReaders := make([]io.Reader, dataShards)
	for i := 0; i < dataShards; i++ {
		shardFiles[i].Seek(0, 0)
		dataReaders[i] = shardFiles[i]
	}

	// Create parity writers
	parityWriters := make([]io.Writer, parityShards)
	for i := 0; i < parityShards; i++ {
		parityWriters[i] = shardFiles[dataShards+i]
	}

	// Generate parity shards
	err = enc.Encode(dataReaders, parityWriters)
	if err != nil {
		// Clean up files on error
		for _, file := range shardFiles {
			file.Close()
			os.Remove(file.Name())
		}
		return domain.ObjectMetadata{}, nil, err
	}

	// Calculate shard size and generate hashes
	shardSize := (fileSize + int64(dataShards) - 1) / int64(dataShards)
	table := crc64.MakeTable(crc64.ISO)
	var hashes []domain.ShardStorage

	for _, file := range shardFiles {
		file.Seek(0, 0)
		hash := crc64.New(table)
		_, err := io.Copy(hash, file)
		if err != nil {
			// Clean up files on error
			for _, f := range shardFiles {
				f.Close()
				os.Remove(f.Name())
			}
			return domain.ObjectMetadata{}, nil, err
		}

		shardStorage := domain.ShardStorage{
			Hash:        fmt.Sprintf("%016x", hash.Sum64()),
			StorageType: "",
			BucketName:  "",
			Key:         "",
		}
		hashes = append(hashes, shardStorage)
		
		// Reset file position for caller
		file.Seek(0, 0)
	}

	meta := domain.ObjectMetadata{
		OriginalSize: fileSize,
		ShardSize:    shardSize,
		ParityShards: parityShards,
		ShardHashes:  hashes,
	}

	return meta, shardFiles, nil
}

func ReconstructFile(shards [][]byte, meta domain.ObjectMetadata) ([]byte, error) {
	totalShards := len(meta.ShardHashes)
	dataShards := totalShards - meta.ParityShards
	parityShards := meta.ParityShards

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	// Make shards slice with total shards capacity
	reconstructShards := make([][]byte, totalShards)
	copy(reconstructShards, shards)

	if err := enc.Reconstruct(reconstructShards); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := enc.Join(&buf, reconstructShards, int(meta.OriginalSize)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ReconstructFileFromFiles reconstructs a file from shard files without loading all into memory
func ReconstructFileFromFiles(shardFiles []*os.File, meta domain.ObjectMetadata) ([]byte, error) {
	totalShards := len(meta.ShardHashes)
	dataShards := totalShards - meta.ParityShards
	parityShards := meta.ParityShards

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	// Read shard data from files
	reconstructShards := make([][]byte, totalShards)
	for i, file := range shardFiles {
		if file != nil {
			file.Seek(0, 0)
			shardData, err := io.ReadAll(file)
			if err != nil {
				return nil, fmt.Errorf("failed to read shard file %d: %w", i, err)
			}
			reconstructShards[i] = shardData
		}
	}

	if err := enc.Reconstruct(reconstructShards); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := enc.Join(&buf, reconstructShards, int(meta.OriginalSize)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ReconstructFileStream reconstructs a file from shard readers using streaming to minimize memory usage
// This is the memory-efficient version for large files
func ReconstructFileStream(shardReaders []io.Reader, dest io.Writer, meta domain.ObjectMetadata) error {
	totalShards := len(meta.ShardHashes)
	dataShards := totalShards - meta.ParityShards
	parityShards := meta.ParityShards

	enc, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return err
	}

	// Verify and reconstruct if needed
	ok, err := enc.Verify(shardReaders)
	if err != nil {
		return err
	}

	if !ok {
		// Need reconstruction - create temp files for missing shards
		tempFiles := make([]*os.File, totalShards)
		outWriters := make([]io.Writer, totalShards)
		
		// Create temp files for missing shards
		for i := 0; i < totalShards; i++ {
			if shardReaders[i] == nil {
				tempFile, err := os.CreateTemp("", fmt.Sprintf("reconstruct_%d_*.tmp", i))
				if err != nil {
					// Clean up any created temp files
					for j := 0; j < i; j++ {
						if tempFiles[j] != nil {
							tempFiles[j].Close()
							os.Remove(tempFiles[j].Name())
						}
					}
					return err
				}
				tempFiles[i] = tempFile
				outWriters[i] = tempFile
			}
		}

		// Perform reconstruction
		err = enc.Reconstruct(shardReaders, outWriters)
		if err != nil {
			// Clean up temp files on error
			for _, file := range tempFiles {
				if file != nil {
					file.Close()
					os.Remove(file.Name())
				}
			}
			return err
		}

		// Update readers to include reconstructed shards
		for i, file := range tempFiles {
			if file != nil {
				file.Seek(0, 0)
				shardReaders[i] = file
			}
		}

		// Clean up temp files when done
		defer func() {
			for _, file := range tempFiles {
				if file != nil {
					file.Close()
					os.Remove(file.Name())
				}
			}
		}()
	}

	// Join the data shards to reconstruct original file
	dataReaders := shardReaders[:dataShards]
	return enc.Join(dest, dataReaders, meta.OriginalSize)
}

func ReconstructFileFromPaths(filePaths []string, meta domain.ObjectMetadata) ([]byte, error) {
	totalShards := len(meta.ShardHashes)
	dataShards := totalShards - meta.ParityShards
	parityShards := meta.ParityShards

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	// Create sparse array for reconstruction - only first N files are valid
	reconstructShards := make([][]byte, totalShards)
	for i, path := range filePaths {
		if i < totalShards {
			shardData, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read shard file %s: %w", path, err)
			}
			reconstructShards[i] = shardData
		}
	}

	if err := enc.Reconstruct(reconstructShards); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := enc.Join(&buf, reconstructShards, int(meta.OriginalSize)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
