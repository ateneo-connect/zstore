package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

func BenchmarkFileService_UploadFile(b *testing.B) {
	// Load config
	cfg, err := config.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	dynamoDb, err := db.NewDatabase(cfg)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize repositories
	s3Store := objectstore.NewS3ObjectStore(cfg)
	s3ObjectRepository := objectstore.NewS3ObjectRepository(s3Store.Client, cfg.S3BucketName)
	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)
	
	// Initialize file service
	fileService := service.NewFileService(&s3ObjectRepository, &metadataRepository)

	// Test data sizes
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
			// Generate test data
			data := make([]byte, size.size)
			rand.Read(data)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := "benchmark/test-file"
				reader := bytes.NewReader(data)
				
				err := fileService.UploadFile(context.Background(), key, reader, true, 4, 2)
				if err != nil {
					b.Fatalf("UploadFile failed: %v", err)
				}

				// Clean up
				fileService.DeleteFile(context.Background(), key)
			}
		})
	}
}

func BenchmarkFileService_DownloadFile(b *testing.B) {
	// Load config
	cfg, err := config.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	dynamoDb, err := db.NewDatabase(cfg)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize repositories
	s3Store := objectstore.NewS3ObjectStore(cfg)
	s3ObjectRepository := objectstore.NewS3ObjectRepository(s3Store.Client, cfg.S3BucketName)
	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)
	
	// Initialize file service
	fileService := service.NewFileService(&s3ObjectRepository, &metadataRepository)

	// Test data sizes
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
			// Setup: Upload test file
			data := make([]byte, size.size)
			rand.Read(data)
			key := "benchmark/download-test-file"
			
			err := fileService.UploadFile(context.Background(), key, bytes.NewReader(data), true, 4, 2)
			if err != nil {
				b.Fatalf("Setup failed: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader, err := fileService.DownloadFile(context.Background(), key, true)
				if err != nil {
					b.Fatalf("DownloadFile failed: %v", err)
				}
				reader.Close()
			}

			// Cleanup
			fileService.DeleteFile(context.Background(), key)
		})
	}
}

func BenchmarkFileService_ConcurrencyComparison(b *testing.B) {
	// Load config
	cfg, err := config.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	dynamoDb, err := db.NewDatabase(cfg)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize repositories
	s3Store := objectstore.NewS3ObjectStore(cfg)
	s3ObjectRepository := objectstore.NewS3ObjectRepository(s3Store.Client, cfg.S3BucketName)
	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)

	// Test different concurrency levels
	concurrencyLevels := []int{1, 2, 3, 5}
	
	// 1MB test data
	data := make([]byte, 1024*1024)
	rand.Read(data)

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			fileService := service.NewFileService(&s3ObjectRepository, &metadataRepository)
			// Set concurrency directly on the struct field
			fileService.SetConcurrency(concurrency)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := "benchmark/concurrency-test"
				reader := bytes.NewReader(data)
				
				err := fileService.UploadFile(context.Background(), key, reader, true, 4, 2)
				if err != nil {
					b.Fatalf("UploadFile failed: %v", err)
				}

				// Clean up
				fileService.DeleteFile(context.Background(), key)
			}
		})
	}
}