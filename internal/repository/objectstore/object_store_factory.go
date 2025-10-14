// Package objectstore provides object storage repository implementations and factory.
package objectstore

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectRepository defines the interface for object storage operations
type ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader, quiet bool) (string, error)
	Download(ctx context.Context, key string, quiet bool) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
	GetBucketName() string
	GetStorageType() string
}

// RepositoryType represents the type of object storage
type RepositoryType string

const (
	S3Type  RepositoryType = "s3"
	GCSType RepositoryType = "gcs"
	// Add more types as needed
)

// BucketConfig holds configuration for a storage bucket
type BucketConfig struct {
	Name string
	Type RepositoryType
	// Add provider-specific config fields as needed
}

// ObjectRepositoryFactory creates object repository instances
type ObjectRepositoryFactory struct {
	awsConfig aws.Config
	gcsClient *storage.Client
	// Add other provider configs as needed
}

// NewObjectRepositoryFactory creates a new factory
func NewObjectRepositoryFactory(awsConfig aws.Config, gcsClient *storage.Client) *ObjectRepositoryFactory {
	return &ObjectRepositoryFactory{
		awsConfig: awsConfig,
		gcsClient: gcsClient,
	}
}

// CreateRepository creates a repository based on bucket configuration
func (f *ObjectRepositoryFactory) CreateRepository(config BucketConfig) (ObjectRepository, error) {
	switch config.Type {
	case S3Type:
		client := s3.NewFromConfig(f.awsConfig)
		repo := NewS3ObjectRepository(client, config.Name)
		return &repo, nil
	case GCSType:
		if f.gcsClient == nil {
			return nil, fmt.Errorf("GCS client not configured")
		}
		repo := NewGCSObjectRepository(f.gcsClient, config.Name)
		return &repo, nil
	default:
		return nil, fmt.Errorf("unsupported repository type: %s", config.Type)
	}
}

// ParseBucketConfig parses bucket configuration from string
// Formats: "s3://bucket-name", "gs://bucket-name", "s3:bucket-name", or "bucket-name" (defaults to S3)
func ParseBucketConfig(bucketStr string) (BucketConfig, error) {
	bucketStr = strings.TrimSpace(bucketStr)

	// Handle URI format (s3://, gs://)
	if strings.Contains(bucketStr, "://") {
		parts := strings.SplitN(bucketStr, "://", 2)
		if len(parts) != 2 {
			return BucketConfig{}, fmt.Errorf("invalid URI format: %s", bucketStr)
		}

		scheme := strings.ToLower(strings.TrimSpace(parts[0]))
		bucketName := strings.TrimSpace(parts[1])

		if bucketName == "" {
			return BucketConfig{}, fmt.Errorf("bucket name cannot be empty")
		}

		var repoType RepositoryType
		switch scheme {
		case "s3":
			repoType = S3Type
		case "gs":
			repoType = GCSType
		default:
			return BucketConfig{}, fmt.Errorf("unsupported scheme: %s", scheme)
		}

		return BucketConfig{
			Name: bucketName,
			Type: repoType,
		}, nil
	}

	// Handle colon format (s3:bucket-name)
	parts := strings.SplitN(bucketStr, ":", 2)
	if len(parts) != 2 {
		// Default to S3 for backward compatibility
		return BucketConfig{
			Name: bucketStr,
			Type: S3Type,
		}, nil
	}

	repoType := RepositoryType(strings.ToLower(strings.TrimSpace(parts[0])))
	bucketName := strings.TrimSpace(parts[1])

	if bucketName == "" {
		return BucketConfig{}, fmt.Errorf("bucket name cannot be empty")
	}

	return BucketConfig{
		Name: bucketName,
		Type: repoType,
	}, nil
}
