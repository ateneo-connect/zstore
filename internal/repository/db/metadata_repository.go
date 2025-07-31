package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/zzenonn/zstore/internal/domain"
)

// MetadataRepository manages DynamoDB interactions for ObjectMetadata.
type MetadataRepository struct {
	client    *dynamodb.Client
	tableName string
}

// NewMetadataRepository initializes a new MetadataRepository.
func NewMetadataRepository(client *dynamodb.Client, tableName string) MetadataRepository {
	return MetadataRepository{
		client:    client,
		tableName: tableName,
	}
}

// CreateMetadata stores object metadata in DynamoDB.
func (repo *MetadataRepository) CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error) {
	metadataMap, err := attributevalue.MarshalMap(metadata)
	if err != nil {
		return domain.ObjectMetadata{}, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(repo.tableName),
		Item:      metadataMap,
	}

	if _, err := repo.client.PutItem(ctx, input); err != nil {
		return domain.ObjectMetadata{}, fmt.Errorf("failed to create metadata: %w", err)
	}

	return metadata, nil
}

// GetMetadata retrieves object metadata by prefix and filename.
func (repo *MetadataRepository) GetMetadata(ctx context.Context, prefix, fileName string) (domain.ObjectMetadata, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(repo.tableName),
		Key: map[string]types.AttributeValue{
			"prefix":    &types.AttributeValueMemberS{Value: prefix},
			"file_name": &types.AttributeValueMemberS{Value: fileName},
		},
	}

	result, err := repo.client.GetItem(ctx, input)
	if err != nil {
		return domain.ObjectMetadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	if result.Item == nil {
		return domain.ObjectMetadata{}, errors.New("metadata not found")
	}

	var metadata domain.ObjectMetadata
	if err := attributevalue.UnmarshalMap(result.Item, &metadata); err != nil {
		return domain.ObjectMetadata{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return metadata, nil
}

// ListMetadataByPrefix retrieves all object metadata within a specific prefix (directory).
func (repo *MetadataRepository) ListMetadataByPrefix(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(repo.tableName),
		KeyConditionExpression: aws.String("#prefix = :prefix"),
		ExpressionAttributeNames: map[string]string{
			"#prefix": "prefix",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":prefix": &types.AttributeValueMemberS{Value: prefix},
		},
	}

	result, err := repo.client.Query(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata by prefix: %w", err)
	}

	var metadataList []domain.ObjectMetadata
	for _, item := range result.Items {
		var metadata domain.ObjectMetadata
		if err := attributevalue.UnmarshalMap(item, &metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
		metadataList = append(metadataList, metadata)
	}

	return metadataList, nil
}

// UpdateMetadata replaces existing object metadata (full replacement as preferred).
func (repo *MetadataRepository) UpdateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error) {
	// Use PutItem for full replacement as specified in requirements
	return repo.CreateMetadata(ctx, metadata)
}

// DeleteMetadata removes object metadata by prefix and filename.
func (repo *MetadataRepository) DeleteMetadata(ctx context.Context, prefix, fileName string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.tableName),
		Key: map[string]types.AttributeValue{
			"prefix":    &types.AttributeValueMemberS{Value: prefix},
			"file_name": &types.AttributeValueMemberS{Value: fileName},
		},
	}

	if _, err := repo.client.DeleteItem(ctx, input); err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	return nil
}