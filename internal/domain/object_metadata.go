package domain

// ObjectMetadata - representation of an erasure coded object's metadata
type ObjectMetadata struct {
	Prefix       string `json:"prefix" dynamodbav:"pk"` // File path
	FileName     string `json:"file_name" dynamodbav:"file_name"`
	OriginalSize int64  `json:"original_size" dynamodbav:"original_size"`
	ShardSize    int64  `json:"shard_size" dynamodbav:"shard_size"`
	// ShardOrder   []string `json:"shard_order" dynamodbav:"shard_order"` // Redundant if ShardHashes is already ordered
	ShardHashes []string `json:"shard_hashes" dynamodbav:"shard_hashes"`
}
