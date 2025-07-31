package service

import (
	"context"
	"io"
)

type S3ObjectRepository interface {
	Upload(ctx context.Context, key string, r io.Reader) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

type FileService struct {
	repo S3ObjectRepository
}

// NewFileService creates a new FileService instance
func NewFileService(repo S3ObjectRepository) *FileService {
	return &FileService{
		repo: repo,
	}
}

// UploadFile uploads a file to S3
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader) error {
	return s.repo.Upload(ctx, key, r)
}

// DownloadFile downloads a file from S3
func (s *FileService) DownloadFile(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.repo.Download(ctx, key)
}

// DeleteFile deletes a file from S3
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	return s.repo.Delete(ctx, key)
}
