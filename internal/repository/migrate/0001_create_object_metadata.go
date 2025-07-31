package migrate

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ObjectMetadataTableName = "object_metadata"
	ObjectMetadataVersion   = "20250731000000_object_metadata_table"
)

type CreateObjectMetadataTable struct{}

func (m *CreateObjectMetadataTable) Version() string {
	return ObjectMetadataVersion
}

func (m *CreateObjectMetadataTable) TableName() string {
	return ObjectMetadataTableName
}

func (m *CreateObjectMetadataTable) Up(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("prefix"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("file_name"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("prefix"),
				KeyType:       types.KeyTypeHash, // Partition Key
			},
			{
				AttributeName: aws.String("file_name"),
				KeyType:       types.KeyTypeRange, // Sort Key
			},
		},
		TableName:   aws.String(ObjectMetadataTableName),
		BillingMode: types.BillingModePayPerRequest, // On-demand billing for variable workloads
		Tags: []types.Tag{
			{
				Key:   aws.String("Purpose"),
				Value: aws.String("ErasureCodingMetadata"),
			},
			{
				Key:   aws.String("Environment"),
				Value: aws.String("Development"),
			},
		},
	}

	// Create the table
	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return err
	}

	// Wait for table to become active
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(ObjectMetadataTableName),
	}, 5*time.Minute)

	return err
}

func (m *CreateObjectMetadataTable) Down(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(ObjectMetadataTableName),
	}

	_, err := client.DeleteTable(ctx, input)
	return err
}
