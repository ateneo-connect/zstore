package db

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
)

type DynamoDb struct {
	Client        *dynamodb.Client
	TaggingClient *resourcegroupstaggingapi.Client
}

func NewDatabase(cfg *config.Config) (*DynamoDb, error) {
	client := dynamodb.NewFromConfig(cfg.AwsConfig)
	if client == nil {
		log.Fatal("Failed to create DynamoDB client")
	}

	taggingClient := resourcegroupstaggingapi.NewFromConfig(cfg.AwsConfig)
	if taggingClient == nil {
		log.Fatal("Failed to create Resource Groups Tagging API client")
	}

	return &DynamoDb{
		Client:        client,
		TaggingClient: taggingClient,
	}, nil
}
