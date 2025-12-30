package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/placement"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

func setupFileService(t *testing.T) *service.FileService {
	rootCmd := &cobra.Command{}
	configPath := os.Getenv("ZSTORE_CONFIG_PATH")
	if configPath == "" {
		t.Fatalf("ZSTORE_CONFIG_PATH environment variable must be set")
	}
	cfg, err := config.LoadConfig(configPath, rootCmd)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	dynamoDb, err := db.NewDatabase(cfg.AwsConfig)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	factory := objectstore.NewObjectRepositoryFactory(cfg.AwsConfig, cfg.GcsClient)
	placer := placement.NewRoundRobinPlacer()

	for bucketKey, bucketConfig := range cfg.Buckets {
		repoConfig := objectstore.BucketConfig{
			Name: bucketConfig.BucketName,
			Type: objectstore.RepositoryType(bucketConfig.Platform),
		}
		repo, err := factory.CreateRepository(repoConfig)
		if err != nil {
			t.Fatalf("Failed to create repository: %v", err)
		}
		placer.RegisterBucket(bucketKey, repo)
	}

	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)
	return service.NewFileService(placer, &metadataRepository)
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
			originalData := make([]byte, tc.size)
			_, err := rand.Read(originalData)
			if err != nil {
				t.Fatalf("Failed to generate test data: %v", err)
			}

			originalHash := sha256.Sum256(originalData)
			key := "integration-test/test-file.bin"

			err = fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, 4, 2, 3)
			if err != nil {
				t.Fatalf("UploadFile failed: %v", err)
			}

			tempFile, err := os.CreateTemp("", "test_*.tmp")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tempFile.Name())
			
			err = fileService.DownloadFile(context.Background(), key, tempFile, true)
			if err != nil {
				t.Fatalf("DownloadFile failed: %v", err)
			}
			
			tempFile.Seek(0, 0)
			downloadedData, err := io.ReadAll(tempFile)
			tempFile.Close()
			if err != nil {
				t.Fatalf("Failed to read temp file: %v", err)
			}
			downloadedHash := sha256.Sum256(downloadedData)
			if originalHash != downloadedHash {
				t.Errorf("Data integrity check failed: original hash %x != downloaded hash %x", originalHash, downloadedHash)
			}

			if len(originalData) != len(downloadedData) {
				t.Errorf("Size mismatch: original %d != downloaded %d", len(originalData), len(downloadedData))
			}

			err = fileService.DeleteFile(context.Background(), key)
			if err != nil {
				t.Fatalf("DeleteFile failed: %v", err)
			}

			tempFile2, _ := os.CreateTemp("", "test2_*.tmp")
			defer os.Remove(tempFile2.Name())
			err = fileService.DownloadFile(context.Background(), key, tempFile2, true)
			tempFile2.Close()
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

	originalData := make([]byte, 10*1024)
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	originalHash := sha256.Sum256(originalData)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := "integration-test/shard-config-test.bin"

			err = fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, tc.dataShards, tc.parityShards, 3)
			if err != nil {
				t.Fatalf("UploadFile failed: %v", err)
			}

			tempFile, err := os.CreateTemp("", "test_*.tmp")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tempFile.Name())
			
			err = fileService.DownloadFile(context.Background(), key, tempFile, true)
			if err != nil {
				t.Fatalf("DownloadFile failed: %v", err)
			}
			
			tempFile.Seek(0, 0)
			downloadedData, err := io.ReadAll(tempFile)
			tempFile.Close()
			if err != nil {
				t.Fatalf("Failed to read temp file: %v", err)
			}

			downloadedHash := sha256.Sum256(downloadedData)
			if originalHash != downloadedHash {
				t.Errorf("Data integrity check failed for %s", tc.name)
			}

			fileService.DeleteFile(context.Background(), key)
		})
	}
}

func TestFileService_ConcurrentOperations(t *testing.T) {
	fileService := setupFileService(t)
	fileService.SetConcurrency(3)

	originalData := make([]byte, 50*1024)
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	originalHash := sha256.Sum256(originalData)

	numFiles := 5
	keys := make([]string, numFiles)
	
	for i := 0; i < numFiles; i++ {
		keys[i] = fmt.Sprintf("integration-test/concurrent-test-%d.bin", i)
		
		err = fileService.UploadFile(context.Background(), keys[i], bytes.NewReader(originalData), true, 4, 2, 3)
		if err != nil {
			t.Fatalf("UploadFile %d failed: %v", i, err)
		}
	}

	for i, key := range keys {
		tempFile, err := os.CreateTemp("", "test_*.tmp")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())
		
		err = fileService.DownloadFile(context.Background(), key, tempFile, true)
		if err != nil {
			t.Fatalf("DownloadFile %d failed: %v", i, err)
		}
		
		tempFile.Seek(0, 0)
		downloadedData, err := io.ReadAll(tempFile)
		tempFile.Close()
		if err != nil {
			t.Fatalf("Failed to read temp file: %v", err)
		}

		downloadedHash := sha256.Sum256(downloadedData)
		if originalHash != downloadedHash {
			t.Errorf("Data integrity check failed for file %d", i)
		}
	}

	for _, key := range keys {
		fileService.DeleteFile(context.Background(), key)
	}
}

func TestFileService_EmptyFile(t *testing.T) {
	fileService := setupFileService(t)
	
	originalData := []byte{}
	key := "integration-test/empty-file.bin"

	err := fileService.UploadFile(context.Background(), key, bytes.NewReader(originalData), true, 4, 2, 3)
	if err == nil {
		t.Error("Expected UploadFile to fail for empty file, but it succeeded")
	}
}

func TestFileService_AutoDetectFilename(t *testing.T) {
	fileService := setupFileService(t)
	
	// Create test data
	originalData := make([]byte, 1024)
	_, err := rand.Read(originalData)
	if err != nil {
		t.Fatalf("Failed to generate test data: %v", err)
	}
	originalHash := sha256.Sum256(originalData)
	
	// Test with source filename "test-file.txt" - should auto-detect to "test-file.txt" in bucket root
	sourceFilename := "test-file.txt"
	expectedKey := filepath.Base(sourceFilename) // This simulates CLI auto-detection logic
	
	// Upload with auto-detected filename (simulating CLI behavior)
	err = fileService.UploadFile(context.Background(), expectedKey, bytes.NewReader(originalData), true, 4, 2, 3)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	
	// Download using the expected key
	tempFile, err := os.CreateTemp("", "test_*.tmp")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	
	err = fileService.DownloadFile(context.Background(), expectedKey, tempFile, true)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}
	
	tempFile.Seek(0, 0)
	downloadedData, err := io.ReadAll(tempFile)
	tempFile.Close()
	if err != nil {
		t.Fatalf("Failed to read temp file: %v", err)
	}
	
	downloadedHash := sha256.Sum256(downloadedData)
	if originalHash != downloadedHash {
		t.Errorf("Data integrity check failed: original hash %x != downloaded hash %x", originalHash, downloadedHash)
	}
	
	// Cleanup
	err = fileService.DeleteFile(context.Background(), expectedKey)
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}
}