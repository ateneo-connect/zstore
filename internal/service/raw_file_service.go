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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
)

// RawFileService provides direct file operations without erasure coding
// TODO: Add support for other storage providers (GCS, Azure) beyond S3
type RawFileService struct {
	awsConfig aws.Config
}

// NewRawFileService creates a new RawFileService for simple raw operations
func NewRawFileService(awsConfig aws.Config) *RawFileService {
	return &RawFileService{
		awsConfig: awsConfig,
	}
}

// UploadFileRaw uploads a file directly to S3 without erasure coding
func (r *RawFileService) UploadFileRaw(ctx context.Context, bucketName, key string, reader io.Reader, quiet bool) error {
	log.Debugf("Uploading raw file %s to bucket %s", key, bucketName)
	
	// Create S3 client and uploader
	s3Client := s3.NewFromConfig(r.awsConfig)
	uploader := manager.NewUploader(s3Client)
	
	// Create progress bar if not quiet
	var progressReader io.Reader = reader
	if !quiet {
		// Try to get file size for progress bar
		if seeker, ok := reader.(io.Seeker); ok {
			size, err := seeker.Seek(0, io.SeekEnd)
			if err == nil {
				seeker.Seek(0, io.SeekStart)
				bar := progressbar.DefaultBytes(size, "uploading")
				pbReader := progressbar.NewReader(reader, bar)
				progressReader = &pbReader
			}
		}
	}
	
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   progressReader,
	})
	return err
}

// DownloadFileRaw downloads a file directly from S3 without erasure coding reconstruction
func (r *RawFileService) DownloadFileRaw(ctx context.Context, bucketName, key string, quiet bool) (io.ReadCloser, error) {
	log.Debugf("Downloading raw file %s from bucket %s", key, bucketName)
	
	// Create S3 client
	s3Client := s3.NewFromConfig(r.awsConfig)
	
	// Get object info first to determine size for progress bar
	if !quiet {
		headResult, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucketName,
			Key:    &key,
		})
		if err == nil && headResult.ContentLength != nil {
			// Get the object with progress tracking
			result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucketName,
				Key:    &key,
			})
			if err != nil {
				return nil, err
			}
			
			// Wrap with progress bar
			bar := progressbar.DefaultBytes(*headResult.ContentLength, "downloading")
			progressReader := progressbar.NewReader(result.Body, bar)
			return io.NopCloser(&progressReader), nil
		}
	}
	
	// Fallback to simple download without progress
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	
	return result.Body, nil
}