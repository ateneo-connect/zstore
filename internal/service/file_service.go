package service

import (
	"context"
	"io"
)

type UserProfileRepository interface {
	Upload(ctx context.Context, key string, r io.Reader) error
	GetPresignedUrl(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) error
}

type UserProfileService struct {
	repo UserProfileRepository
}

// NewUserProfileService creates a new UserProfileService instance
func NewUserProfileService(repo UserProfileRepository) *UserProfileService {
	return &UserProfileService{
		repo: repo,
	}
}

// UploadProfile uploads a user profile image
func (s *UserProfileService) UploadProfile(ctx context.Context, username string, key string, r io.Reader) error {
	return s.repo.Upload(ctx, username+"/profile/"+key, r)
}

// DownloadProfile downloads a user profile image
func (s *UserProfileService) GeneratePresignedURL(ctx context.Context, username string, key string) (string, error) {
	return s.repo.GetPresignedUrl(ctx, username+"/profile/"+key)
}

// DeleteProfile deletes a user profile image
func (s *UserProfileService) DeleteProfile(ctx context.Context, username string, key string) error {
	return s.repo.Delete(ctx, username+"/profile/"+key)
}
