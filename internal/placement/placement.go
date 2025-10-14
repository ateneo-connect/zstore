// Package placement provides intelligent shard placement functionality for erasure coding systems.
//
// This package manages the distribution of erasure-coded shards across multiple storage backends
// (S3, GCS, etc.) to achieve fault tolerance, load distribution, and optimal performance.
//
// Key Concepts:
// - Shard Placement: Determines which storage bucket/backend each shard is stored in
// - Round-Robin Distribution: Evenly distributes shards across available buckets
// - Multi-Provider Support: Can mix different storage providers (S3 + GCS)
// - Fault Tolerance: If one bucket fails, shards in other buckets remain accessible
//
// Architecture Role:
// The placement package sits between the service layer (business logic) and repository layer
// (storage implementations). It abstracts away the complexity of managing multiple storage
// backends and provides a unified interface for shard distribution.
//
// Usage Flow:
// 1. Configuration creates placement system with multiple buckets
// 2. FileService uses Placer to place shards across buckets during upload
// 3. During download, Placer routes requests to correct buckets based on metadata
// 4. Round-robin ensures even distribution: shard 0 → bucket A, shard 1 → bucket B, etc.
//
// Example:
//
//	placer := NewRoundRobinPlacer()
//	placer.RegisterBucket("s3-bucket-1", s3Repo1)
//	placer.RegisterBucket("gcs-bucket-2", gcsRepo2)
//
//	// Upload: FileService calls Place(shardIndex)
//	bucketName, repo, _ := placer.Place(0) // Returns s3-bucket-1
//	bucketName, repo, _ := placer.Place(1) // Returns gcs-bucket-2
//
//	// Download: FileService calls GetRepositoryForBucket(bucketName)
//	repo, _ := placer.GetRepositoryForBucket("s3-bucket-1")
//
// Benefits:
// - Load Distribution: Spreads I/O load across multiple storage backends
// - Fault Tolerance: Reed-Solomon can reconstruct data even if some buckets are unavailable
// - Cost Optimization: Can use different storage tiers/providers based on requirements
// - Scalability: Easy to add more buckets as storage needs grow
package placement

import (
	"github.com/zzenonn/zstore/internal/repository/objectstore"
)

// Placer manages shard placement across multiple storage backends.
//
// Provides pluggable algorithms for distributing erasure-coded shards:
// - Round-Robin: Even distribution across buckets
// - Weighted: Distribution based on bucket capacity/performance
// - Geographic: Placement based on regions for latency optimization
// - Performance: Route to fastest available buckets
//
// Implementations must be thread-safe and deterministic (same shardIndex
// should generally return the same bucket for consistent reconstruction).
type Placer interface {
	// GetRepositoryForBucket returns the repository for a specific bucket.
	// Used during downloads when bucket is known from metadata.
	GetRepositoryForBucket(bucketName string) (objectstore.ObjectRepository, error)

	// Place selects the optimal bucket for a shard based on placement algorithm.
	// Core method that implements the placement strategy using shardIndex.
	Place(shardIndex int) (string, objectstore.ObjectRepository, error)

	// RegisterBucket adds a storage bucket and repository to the placer.
	// Called during initialization to configure available storage backends.
	RegisterBucket(bucketName string, repo objectstore.ObjectRepository) error

	// ListBuckets returns all registered bucket names.
	// Used for administrative operations like cleanup across all buckets.
	ListBuckets() []string
}
