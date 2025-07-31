package service

import (
	"context"
	"io"
	"path/filepath"

	"github.com/zzenonn/zstore/internal/domain"
)

type S3ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

type MetadataRepository interface {
	CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
}

type FileService struct {
	repo         S3ObjectRepository
	metadataRepo MetadataRepository
}

// NewFileService creates a new FileService instance
func NewFileService(repo S3ObjectRepository, metadataRepo MetadataRepository) *FileService {
	return &FileService{
		repo:         repo,
		metadataRepo: metadataRepo,
	}
}

// UploadFile uploads a file to S3
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader) error {

	// Create metadata with mock data
	prefix := filepath.Dir(key)
	if prefix == "." {
		prefix = ""
	}
	fileName := filepath.Base(key)

	metadata := domain.ObjectMetadata{
		Prefix:       prefix,
		FileName:     fileName,
		OriginalSize: 1024000,                                      // Mock: 1MB
		ShardSize:    128000,                                       // Mock: 128KB
		ShardHashes:  []string{"hash1", "hash2", "hash3", "hash4"}, // Mock hashes
	}
	_, err := s.metadataRepo.CreateMetadata(ctx, metadata)

	if err := s.repo.Upload(ctx, key, r); err != nil {
		return err
	}
	return err
}

// DownloadFile downloads a file from S3
func (s *FileService) DownloadFile(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.repo.Download(ctx, key)
}

// DeleteFile deletes a file from S3
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	return s.repo.Delete(ctx, key)
}
