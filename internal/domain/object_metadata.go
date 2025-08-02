package domain

// ShardStorage - storage information for a shard
type ShardStorage struct {
	Hash        string `json:"hash" dynamodbav:"hash"`
	StorageType string `json:"storage_type" dynamodbav:"storage_type"`
	BucketName  string `json:"bucket_name" dynamodbav:"bucket_name"`
	Key         string `json:"key" dynamodbav:"key"`
}

// ObjectMetadata - representation of an erasure coded object's metadata
type ObjectMetadata struct {
	Prefix       string         `json:"prefix" dynamodbav:"prefix"`           // Directory path - Partition Key
	FileName     string         `json:"file_name" dynamodbav:"file_name"`     // Filename - Sort Key
	OriginalSize int64          `json:"original_size" dynamodbav:"original_size"`
	ShardSize    int64          `json:"shard_size" dynamodbav:"shard_size"`
	ParityShards int            `json:"parity_shards" dynamodbav:"parity_shards"`
	ShardHashes  []ShardStorage `json:"shard_hashes" dynamodbav:"shard_hashes"` // Ordered array of shard storage info
}
