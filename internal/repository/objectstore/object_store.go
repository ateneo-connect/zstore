package objectstore

import (
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

type S3Store struct {
	Client        *s3.Client
	TaggingClient *resourcegroupstaggingapi.Client
}

func NewS3ObjectStore(awsConfig aws.Config) *S3Store {
	client := s3.NewFromConfig(awsConfig)
	if client == nil {
		log.Fatal("Failed to create S3 client")
	}

	taggingClient := resourcegroupstaggingapi.NewFromConfig(awsConfig)
	if taggingClient == nil {
		log.Fatal("Failed to create Resource Groups Tagging API client")
	}

	return &S3Store{
		Client:        client,
		TaggingClient: taggingClient,
	}
}

// NewS3ObjectRepository creates a new S3 object repository
func NewS3ObjectRepository(client *s3.Client, bucketName string) S3ObjectRepository {
	return S3ObjectRepository{
		client:     client,
		bucketName: bucketName,
	}
}

// NewGCSObjectRepository creates a new GCS object repository
func NewGCSObjectRepository(client *storage.Client, bucketName string) GCSObjectRepository {
	return GCSObjectRepository{
		client:     client,
		bucketName: bucketName,
		downloader: nil, // Will be initialized on first use
	}
}
