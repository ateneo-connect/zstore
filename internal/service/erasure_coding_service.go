package service

import (
	"bytes"
	"hash/crc64"
	"fmt"
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