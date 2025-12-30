// Package service provides the core business logic for the erasure coding object storage system.
// This file implements the RawFileService for direct file operations without erasure coding.
//
// RawFileService provides simple, direct file operations:
// - Single-bucket upload/download without erasure coding
// - Direct repository access for simple use cases
// - No metadata or sharding overhead
//
// Key Operations:
// - UploadToS3/UploadToGCS: Direct upload to storage without sharding
// - DownloadFromS3/DownloadFromGCS: Direct download from storage without reconstruction
//
// Use Cases:
// - Simple file storage without fault tolerance requirements
// - Legacy system integration
// - Testing and development scenarios
// - Backup of non-critical files
package service

import (
	"context"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
)

// RawFileService provides direct file operations without erasure coding using existing repositories
type RawFileService struct {
	factory *objectstore.ObjectRepositoryFactory
}

// NewRawFileService creates a new RawFileService that uses the repository factory
func NewRawFileService(factory *objectstore.ObjectRepositoryFactory) *RawFileService {
	return &RawFileService{
		factory: factory,
	}
}

// UploadToRepository uploads a file directly to a repository without erasure coding
func (r *RawFileService) UploadToRepository(ctx context.Context, bucketName, key string, reader io.Reader, quiet bool) error {
	log.Debugf("Uploading raw file %s to bucket %s", key, bucketName)

	// Create repository for this bucket on-demand
	repo, err := r.createRepositoryForBucket(bucketName)
	if err != nil {
		return err
	}

	_, err = repo.Upload(ctx, key, reader, quiet)
	return err
}

// DownloadFromRepository downloads a file directly from a repository without erasure coding
func (r *RawFileService) DownloadFromRepository(ctx context.Context, bucketName, key string, quiet bool) (io.ReadCloser, error) {
	log.Debugf("Downloading raw file %s from bucket %s", key, bucketName)

	// Create repository for this bucket on-demand
	repo, err := r.createRepositoryForBucket(bucketName)
	if err != nil {
		return nil, err
	}

	return repo.Download(ctx, key, quiet)
}

// DeleteFromRepository deletes a file directly from a repository without erasure coding
func (r *RawFileService) DeleteFromRepository(ctx context.Context, bucketName, key string) error {
	log.Debugf("Deleting raw file %s from bucket %s", key, bucketName)

	// Create repository for this bucket on-demand
	repo, err := r.createRepositoryForBucket(bucketName)
	if err != nil {
		return err
	}

	return repo.Delete(ctx, key)
}

// createRepositoryForBucket creates a repository based on bucket name and URL scheme
func (r *RawFileService) createRepositoryForBucket(bucketName string) (objectstore.ObjectRepository, error) {
	// For now, we need to determine the provider type
	// This could be enhanced to auto-detect or use a registry
	// For simplicity, we'll try S3 first, then GCS

	// Try S3 first
	s3Config := objectstore.BucketConfig{
		Name: bucketName,
		Type: objectstore.S3Type,
	}
	repo, err := r.factory.CreateRepository(s3Config)
	if err == nil {
		return repo, nil
	}

	// Try GCS if S3 fails
	gcsConfig := objectstore.BucketConfig{
		Name: bucketName,
		Type: objectstore.GCSType,
	}
	return r.factory.CreateRepository(gcsConfig)
}
