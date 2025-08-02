package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/service"
)

// mockS3ObjectRepository is a mock implementation of an S3-like object repository for testing.
type mockS3ObjectRepository struct {
	uploadFunc   func(ctx context.Context, key string, r io.Reader) error
	downloadFunc func(ctx context.Context, key string) (io.ReadCloser, error)
	deleteFunc   func(ctx context.Context, key string) error
	storage      map[string][]byte
}

// mockMetadataRepository is a mock implementation of metadata repository for testing.
type mockMetadataRepository struct {
	createFunc func(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
}

func newMockMetadataRepository() *mockMetadataRepository {
	return &mockMetadataRepository{}
}

func (m *mockMetadataRepository) CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, metadata)
	}
	return metadata, nil
}

func newMockS3ObjectRepository() *mockS3ObjectRepository {
	return &mockS3ObjectRepository{
		storage: make(map[string][]byte),
	}
}

func (m *mockS3ObjectRepository) Upload(ctx context.Context, key string, r io.Reader, quiet bool) (string, error) {
	if m.uploadFunc != nil {
		err := m.uploadFunc(ctx, key, r)
		if err != nil {
			return "", err
		}
	}
	return "s3://test-bucket/" + key, nil
}

func (m *mockS3ObjectRepository) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.downloadFunc != nil {
		return m.downloadFunc(ctx, key)
	}
	return io.NopCloser(strings.NewReader("test content")), nil
}

func (m *mockS3ObjectRepository) Delete(ctx context.Context, key string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, key)
	}
	return nil
}

// TestFileService_UploadFile tests the UploadFile method for both success and error cases.
func TestFileService_UploadFile(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		content string
		mockErr error
		wantErr bool
	}{
		{
			name:    "successful upload",
			key:     "test/file.txt",
			content: "test content",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:    "upload error",
			key:     "test/file.txt",
			content: "test content",
			mockErr: errors.New("upload failed"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := &mockS3ObjectRepository{
				uploadFunc: func(ctx context.Context, key string, r io.Reader) error {
					return tt.mockErr
				},
			}

			mockMetaRepo := newMockMetadataRepository()
			fs := service.NewFileService(mockRepo, mockMetaRepo)
			reader := strings.NewReader(tt.content)

			err := fs.UploadFile(context.Background(), tt.key, reader, false)

			if (err != nil) != tt.wantErr {
				t.Errorf("UploadFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestFileService_DownloadFile tests the DownloadFile method for both success and error cases.
func TestFileService_DownloadFile(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		mockErr error
		wantErr bool
	}{
		{
			name:    "successful download",
			key:     "test/file.txt",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:    "download error",
			key:     "test/file.txt",
			mockErr: errors.New("download failed"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := &mockS3ObjectRepository{
				downloadFunc: func(ctx context.Context, key string) (io.ReadCloser, error) {
					if tt.mockErr != nil {
						return nil, tt.mockErr
					}
					return io.NopCloser(strings.NewReader("test content")), nil
				},
			}

			mockMetaRepo := newMockMetadataRepository()
			fs := service.NewFileService(mockRepo, mockMetaRepo)

			reader, err := fs.DownloadFile(context.Background(), tt.key)

			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && reader != nil {
				reader.Close()
			}
		})
	}
}

// TestFileService_DeleteVerification verifies that a file is actually deleted and cannot be downloaded after deletion.
func TestFileService_DeleteVerification(t *testing.T) {
	content := "test content to be deleted"
	key := "test/delete-me.txt"

	mockRepo := newMockS3ObjectRepository()
	mockRepo.uploadFunc = func(ctx context.Context, key string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		mockRepo.storage[key] = data
		return nil
	}
	mockRepo.downloadFunc = func(ctx context.Context, key string) (io.ReadCloser, error) {
		data, exists := mockRepo.storage[key]
		if !exists {
			return nil, errors.New("file not found")
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	mockRepo.deleteFunc = func(ctx context.Context, key string) error {
		delete(mockRepo.storage, key)
		return nil
	}

	mockMetaRepo := newMockMetadataRepository()
	fs := service.NewFileService(mockRepo, mockMetaRepo)

	// Upload file
	reader := strings.NewReader(content)
	err := fs.UploadFile(context.Background(), key, reader, false)
	if err != nil {
		t.Fatalf("UploadFile() failed: %v", err)
	}

	// Verify file exists by downloading
	downloadReader, err := fs.DownloadFile(context.Background(), key)
	if err != nil {
		t.Fatalf("DownloadFile() before delete failed: %v", err)
	}
	downloadReader.Close()

	// Delete file
	err = fs.DeleteFile(context.Background(), key)
	if err != nil {
		t.Fatalf("DeleteFile() failed: %v", err)
	}

	// Verify file no longer exists
	_, err = fs.DownloadFile(context.Background(), key)
	if err == nil {
		t.Error("Expected download to fail after delete, but it succeeded")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got: %v", err)
	}
}

// TestFileService_SmallFileUploadDownload verifies uploading and downloading a small file.
func TestFileService_SmallFileUploadDownload(t *testing.T) {
	content := "test content for small file"
	key := "test/small-file.txt"

	mockRepo := newMockS3ObjectRepository()
	mockRepo.uploadFunc = func(ctx context.Context, key string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		mockRepo.storage[key] = data
		return nil
	}
	mockRepo.downloadFunc = func(ctx context.Context, key string) (io.ReadCloser, error) {
		data, exists := mockRepo.storage[key]
		if !exists {
			return nil, errors.New("file not found")
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	mockMetaRepo := newMockMetadataRepository()
	fs := service.NewFileService(mockRepo, mockMetaRepo)

	// Upload
	reader := strings.NewReader(content)
	err := fs.UploadFile(context.Background(), key, reader, false)
	if err != nil {
		t.Fatalf("UploadFile() failed: %v", err)
	}

	// Download
	downloadReader, err := fs.DownloadFile(context.Background(), key)
	if err != nil {
		t.Fatalf("DownloadFile() failed: %v", err)
	}
	defer downloadReader.Close()

	// Verify content
	downloadedData, err := io.ReadAll(downloadReader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if string(downloadedData) != content {
		t.Errorf("Downloaded content mismatch: got %q, want %q", string(downloadedData), content)
	}
}

// TestFileService_LargeFileUploadDownload verifies uploading and downloading a large file (50MB).
func TestFileService_LargeFileUploadDownload(t *testing.T) {
	const fileSize = 50 * 1024 * 1024 // 50MB

	// Generate 50MB of random data
	originalData := make([]byte, fileSize)
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// Calculate hash of original data for later verification
	originalHash := sha256.Sum256(originalData)

	mockRepo := newMockS3ObjectRepository()
	mockRepo.uploadFunc = func(ctx context.Context, key string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		mockRepo.storage[key] = data
		return nil
	}
	mockRepo.downloadFunc = func(ctx context.Context, key string) (io.ReadCloser, error) {
		data, exists := mockRepo.storage[key]
		if !exists {
			return nil, errors.New("file not found")
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	mockMetaRepo := newMockMetadataRepository()
	fs := service.NewFileService(mockRepo, mockMetaRepo)
	key := "test/large-file.bin"

	// Upload the large file
	reader := bytes.NewReader(originalData)
	err = fs.UploadFile(context.Background(), key, reader, false)
	if err != nil {
		t.Fatalf("UploadFile() failed: %v", err)
	}

	// Download the file
	downloadReader, err := fs.DownloadFile(context.Background(), key)
	if err != nil {
		t.Fatalf("DownloadFile() failed: %v", err)
	}
	defer downloadReader.Close()

	// Read downloaded data
	downloadedData, err := io.ReadAll(downloadReader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	// Verify size
	if len(downloadedData) != fileSize {
		t.Errorf("Downloaded file size mismatch: got %d, want %d", len(downloadedData), fileSize)
	}

	// Verify content by comparing hashes
	downloadedHash := sha256.Sum256(downloadedData)
	if originalHash != downloadedHash {
		t.Error("Downloaded file content does not match original")
	}

	t.Logf("Successfully uploaded and downloaded %d MB file", fileSize/(1024*1024))
}
