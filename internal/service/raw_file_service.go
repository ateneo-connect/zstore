// Package service provides the core business logic for the erasure coding object storage system.
// This file implements the RawFileService for direct file operations without erasure coding.
//
// RawFileService provides simple, direct file operations:
// - Single-bucket upload/download without erasure coding
// - Direct repository access for simple use cases
// - No metadata or sharding overhead
//
// Key Operations:
// - UploadFileRaw: Direct upload to storage without sharding
// - DownloadFileRaw: Direct download from storage without reconstruction
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

// RawFileService provides direct file operations without erasure coding
type RawFileService struct {
	objectRepo objectstore.ObjectRepository
}

// NewRawFileService creates a new RawFileService for simple raw operations
func NewRawFileService(objectRepo objectstore.ObjectRepository) *RawFileService {
	return &RawFileService{
		objectRepo: objectRepo,
	}
}

// UploadFileRaw uploads a file directly to storage without erasure coding
func (r *RawFileService) UploadFileRaw(ctx context.Context, key string, reader io.Reader, quiet bool) error {
	log.Debugf("Uploading raw file %s", key)
	_, err := r.objectRepo.Upload(ctx, key, reader, quiet)
	return err
}

// DownloadFileRaw downloads a file directly from storage without erasure coding reconstruction
func (r *RawFileService) DownloadFileRaw(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	log.Debugf("Downloading raw file %s", key)
	return r.objectRepo.Download(ctx, key, quiet)
}