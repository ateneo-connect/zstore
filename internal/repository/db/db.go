package db

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	log "github.com/sirupsen/logrus"
)

type DynamoDb struct {
	Client        *dynamodb.Client
	TaggingClient *resourcegroupstaggingapi.Client
}

func NewDatabase(awsConfig aws.Config) (*DynamoDb, error) {
	client := dynamodb.NewFromConfig(awsConfig)
	if client == nil {
		log.Fatal("Failed to create DynamoDB client")
	}

	taggingClient := resourcegroupstaggingapi.NewFromConfig(awsConfig)
	if taggingClient == nil {
		log.Fatal("Failed to create Resource Groups Tagging API client")
	}

	return &DynamoDb{
		Client:        client,
		TaggingClient: taggingClient,
	}, nil
}
