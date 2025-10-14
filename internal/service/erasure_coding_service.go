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
//
// Usage:
//   metadata, shards, err := ShardFile(data, 4, 2)  // 4 data + 2 parity shards
//   reconstructed, err := ReconstructFile(shards, metadata)
//
// The service integrates with FileService to provide distributed, fault-tolerant
// file storage across multiple buckets and cloud providers.
package service

import (
	"bytes"
	"fmt"
	"hash/crc64"

	"github.com/klauspost/reedsolomon"
	"github.com/zzenonn/zstore/internal/domain"
)



func ShardFile(data []byte, dataShards, parityShards int) (domain.ObjectMetadata, [][]byte, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return domain.ObjectMetadata{}, nil, err
	}

	shards, err := enc.Split(data)
	if err != nil {
		return domain.ObjectMetadata{}, nil, err
	}

	if err := enc.Encode(shards); err != nil {
		return domain.ObjectMetadata{}, nil, err
	}

	var hashes []domain.ShardStorage
	table := crc64.MakeTable(crc64.ISO)
	for _, shard := range shards {
		crc := crc64.Checksum(shard, table)
		shardStorage := domain.ShardStorage{
			Hash:        fmt.Sprintf("%016x", crc),
			StorageType: "",
			BucketName:  "",
			Key:         "",
		}
		hashes = append(hashes, shardStorage)
	}

	meta := domain.ObjectMetadata{
		OriginalSize: int64(len(data)),
		ShardSize:    int64(len(shards[0])),
		ParityShards: parityShards,
		ShardHashes:  hashes,
	}

	return meta, shards, nil
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
