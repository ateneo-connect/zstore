package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

func setupFileService(t *testing.T) *service.FileService {
	// Load config
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	dynamoDb, err := db.NewDatabase(cfg)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize repositories
	s3Store := objectstore.NewS3ObjectStore(cfg)
	s3ObjectRepository := objectstore.NewS3ObjectRepository(s3Store.Client, cfg.S3BucketName)
	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)
	
	return service.NewFileService(&s3ObjectRepository, &metadataRepository)
}

func TestFileService_UploadDownloadDelete_Integration(t *testing.T) {
	fileService := setupFileService(t)
	
	testCases := []struct {
		name string
		size int
	}{
		{"Small file", 1024},
		{"Medium file", 100 * 1024},
		{"Large file", 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			originalData := make([]byte, tc.size)
			_, err := rand.Read(originalData)
			if err != nil {
				t.Fatalf("Failed to generate test data: %v", err)
			}

			// Calculate original hash
			originalHash := sha256.Sum256(originalData)
			key := "integration-test/test-file.bin"

			// Test Upload
			err = fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, 4, 2)
			if err != nil {
				t.Fatalf("UploadFile failed: %v", err)
			}

			// Test Download
			reader, err := fileService.DownloadFile(context.Background(), key, true)
			if err != nil {
				t.Fatalf("DownloadFile failed: %v", err)
			}
			defer reader.Close()

			// Read downloaded data
			downloadedData, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read downloaded data: %v", err)
			}

			// Verify data integrity using SHA256
			downloadedHash := sha256.Sum256(downloadedData)
			if originalHash != downloadedHash {
				t.Errorf("Data integrity check failed: original hash %x != downloaded hash %x", originalHash, downloadedHash)
			}

			// Verify size
			if len(originalData) != len(downloadedData) {
				t.Errorf("Size mismatch: original %d != downloaded %d", len(originalData), len(downloadedData))
			}

			// Test Delete
			err = fileService.DeleteFile(context.Background(), key)
			if err != nil {
				t.Fatalf("DeleteFile failed: %v", err)
			}

			// Verify deletion by attempting to download again
			_, err = fileService.DownloadFile(context.Background(), key, true)
			if err == nil {
				t.Error("Expected download to fail after deletion, but it succeeded")
			}
		})
	}
}

func TestFileService_UploadDownload_DifferentShardConfigurations(t *testing.T) {
	fileService := setupFileService(t)
	
	testCases := []struct {
		name         string
		dataShards   int
		parityShards int
	}{
		{"2+1 configuration", 2, 1},
		{"4+2 configuration", 4, 2},
		{"6+3 configuration", 6, 3},
	}

	// Generate test data
	originalData := make([]byte, 10*1024) // 10KB
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	originalHash := sha256.Sum256(originalData)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := "integration-test/shard-config-test.bin"

			// Upload
			err = fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, tc.dataShards, tc.parityShards)
			if err != nil {
				t.Fatalf("UploadFile failed: %v", err)
			}

			// Download
			reader, err := fileService.DownloadFile(context.Background(), key, true)
			if err != nil {
				t.Fatalf("DownloadFile failed: %v", err)
			}
			defer reader.Close()

			downloadedData, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read downloaded data: %v", err)
			}

			// Verify integrity
			downloadedHash := sha256.Sum256(downloadedData)
			if originalHash != downloadedHash {
				t.Errorf("Data integrity check failed for %s", tc.name)
			}

			// Cleanup
			fileService.DeleteFile(context.Background(), key)
		})
	}
}

func TestFileService_ConcurrentOperations(t *testing.T) {
	fileService := setupFileService(t)
	fileService.SetConcurrency(3)

	// Generate test data
	originalData := make([]byte, 50*1024) // 50KB
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	originalHash := sha256.Sum256(originalData)

	// Test concurrent uploads and downloads
	numFiles := 5
	keys := make([]string, numFiles)
	
	// Upload multiple files concurrently
	for i := 0; i < numFiles; i++ {
		keys[i] = fmt.Sprintf("integration-test/concurrent-test-%d.bin", i)
		
		err = fileService.UploadFile(context.Background(), keys[i], bytes.NewReader(originalData), true, 4, 2)
		if err != nil {
			t.Fatalf("UploadFile %d failed: %v", i, err)
		}
	}

	// Download and verify all files
	for i, key := range keys {
		reader, err := fileService.DownloadFile(context.Background(), key, true)
		if err != nil {
			t.Fatalf("DownloadFile %d failed: %v", i, err)
		}

		downloadedData, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatalf("Failed to read downloaded data %d: %v", i, err)
		}

		downloadedHash := sha256.Sum256(downloadedData)
		if originalHash != downloadedHash {
			t.Errorf("Data integrity check failed for file %d", i)
		}
	}

	// Cleanup all files
	for _, key := range keys {
		fileService.DeleteFile(context.Background(), key)
	}
}

func TestFileService_EmptyFile(t *testing.T) {
	fileService := setupFileService(t)
	
	// Test empty file
	originalData := []byte{}
	key := "integration-test/empty-file.bin"

	// Upload should return error for empty file
	err := fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, 4, 2)
	if err == nil {
		t.Error("Expected UploadFile to fail for empty file, but it succeeded")
	}
}