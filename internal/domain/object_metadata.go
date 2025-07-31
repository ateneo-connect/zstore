package domain

// ObjectMetadata - representation of an erasure coded object's metadata
type ObjectMetadata struct {
	Prefix       string   `json:"prefix" dynamodbav:"prefix"`           // Directory path - Partition Key
	FileName     string   `json:"file_name" dynamodbav:"file_name"`     // Filename - Sort Key
	OriginalSize int64    `json:"original_size" dynamodbav:"original_size"`
	ShardSize    int64    `json:"shard_size" dynamodbav:"shard_size"`
	ShardHashes  []string `json:"shard_hashes" dynamodbav:"shard_hashes"` // Ordered array of CRC64 hashes
}
