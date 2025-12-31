package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/placement"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

func setupTestServices(b *testing.B) (*service.FileService, *config.Config) {
	rootCmd := &cobra.Command{}
	configPath := os.Getenv("ZSTORE_CONFIG_PATH")
	if configPath == "" {
		b.Fatalf("ZSTORE_CONFIG_PATH environment variable must be set")
	}
	cfg, err := config.LoadConfig(configPath, rootCmd)
	if err != nil {
		b.Fatalf("Failed to load config: %v", err)
	}

	dynamoDb, err := db.NewDatabase(cfg.AwsConfig)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
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
			b.Fatalf("Failed to create repository: %v", err)
		}
		placer.RegisterBucket(bucketKey, repo)
	}

	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)
	fileService := service.NewFileService(placer, &metadataRepository)

	return fileService, cfg
}

func BenchmarkFileService_UploadFile(b *testing.B) {
	fileService, _ := setupTestServices(b)

	testSizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, size := range testSizes {
		b.Run(size.name, func(b *testing.B) {
			data := make([]byte, size.size)
			rand.Read(data)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := "benchmark/test-file"
				reader := bytes.NewReader(data)
				
				err := fileService.UploadFile(context.Background(), key, reader, true, 4, 2, 3)
				if err != nil {
					b.Fatalf("UploadFile failed: %v", err)
				}

				fileService.DeleteFile(context.Background(), key)
			}
		})
	}
}

func BenchmarkFileService_DownloadFile(b *testing.B) {
	fileService, _ := setupTestServices(b)

	testSizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, size := range testSizes {
		b.Run(size.name, func(b *testing.B) {
			data := make([]byte, size.size)
			rand.Read(data)
			key := "benchmark/download-test-file"
			
			err := fileService.UploadFile(context.Background(), key, bytes.NewReader(data), true, 4, 2, 3)
			if err != nil {
				b.Fatalf("Setup failed: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tempFile, err := os.CreateTemp("", "benchmark_*.tmp")
				if err != nil {
					b.Fatalf("Failed to create temp file: %v", err)
				}
				err = fileService.DownloadFile(context.Background(), key, tempFile, true, false)
				tempFile.Close()
				os.Remove(tempFile.Name())
				if err != nil {
					b.Fatalf("DownloadFile failed: %v", err)
				}
			}

			fileService.DeleteFile(context.Background(), key)
		})
	}
}

func BenchmarkFileService_ConcurrencyComparison(b *testing.B) {
	fileService, _ := setupTestServices(b)

	concurrencyLevels := []int{1, 2, 3, 5}
	data := make([]byte, 1024*1024)
	rand.Read(data)

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			fileService.SetConcurrency(concurrency)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := "benchmark/concurrency-test"
				reader := bytes.NewReader(data)
				
				err := fileService.UploadFile(context.Background(), key, reader, true, 4, 2, concurrency)
				if err != nil {
					b.Fatalf("UploadFile failed: %v", err)
				}

				fileService.DeleteFile(context.Background(), key)
			}
		})
	}
}