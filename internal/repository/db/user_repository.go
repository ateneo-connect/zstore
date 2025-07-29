package db

// FOR REVIEW!
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

// UserRepository manages DynamoDB interactions for the User domain.
type UserRepository struct {
	client    *dynamodb.Client
	tableName string
}

// NewUserRepository initializes a new UserRepository.
func NewUserRepository(client *dynamodb.Client, tableName string) UserRepository {
	return UserRepository{
		client:    client,
		tableName: tableName,
	}
}

func (repo *UserRepository) CreateUser(ctx context.Context, user domain.User) (domain.User, error) {
	userMap, err := attributevalue.MarshalMap(user)
	if err != nil {
		return domain.User{}, fmt.Errorf("failed to marshal user: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(repo.tableName),
		Item:      userMap,
	}

	if _, err := repo.client.PutItem(ctx, input); err != nil {
		return domain.User{}, fmt.Errorf("failed to create user: %w", err)
	}

	return user, nil
}

func (repo *UserRepository) GetUser(ctx context.Context, username string) (domain.User, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(repo.tableName),
		KeyConditionExpression: aws.String("pk = :username"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":username": &types.AttributeValueMemberS{Value: username},
		},
		Limit: aws.Int32(1),
	}

	result, err := repo.client.Query(ctx, input)
	if err != nil {
		return domain.User{}, fmt.Errorf("failed to get user: %w", err)
	}

	if len(result.Items) == 0 {
		return domain.User{}, errors.New("user not found")
	}

	var user domain.User
	if err := attributevalue.UnmarshalMap(result.Items[0], &user); err != nil {
		return domain.User{}, fmt.Errorf("failed to unmarshal user data: %w", err)
	}

	return user, nil
}

func (repo *UserRepository) UpdateUser(ctx context.Context, username string, user domain.User) (domain.User, error) {
	userMap, err := attributevalue.MarshalMap(user)
	if err != nil {
		return domain.User{}, fmt.Errorf("failed to marshal user: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(repo.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: username},
		},
		AttributeUpdates: make(map[string]types.AttributeValueUpdate),
	}

	for key, value := range userMap {
		if key != "pk" {
			input.AttributeUpdates[key] = types.AttributeValueUpdate{
				Value:  value,
				Action: types.AttributeActionPut,
			}
		}
	}

	if _, err := repo.client.UpdateItem(ctx, input); err != nil {
		return domain.User{}, fmt.Errorf("failed to update user: %w", err)
	}

	return user, nil
}

func (repo *UserRepository) DeleteUser(ctx context.Context, username string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: username},
		},
	}

	if _, err := repo.client.DeleteItem(ctx, input); err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}
	return nil
}

func (repo *UserRepository) GetUserPassword(ctx context.Context, username string) (domain.User, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(repo.tableName),
		KeyConditionExpression: aws.String("pk = :username"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":username": &types.AttributeValueMemberS{Value: username},
		},
		Limit: aws.Int32(1),
	}

	result, err := repo.client.Query(ctx, input)
	if err != nil {
		return domain.User{}, fmt.Errorf("failed to get user by username: %w", err)
	}

	if result.Items[0] == nil {
		return domain.User{}, errors.New("user not found")
	}

	var user domain.User
	if err := attributevalue.UnmarshalMap(result.Items[0], &user); err != nil {
		return domain.User{}, fmt.Errorf("failed to parse user data: %w", err)
	}

	return user, nil
}

// GetAllUsers retrieves users from DynamoDB using cursor-based pagination.
// Pass a non-nil startKey to get the next page; otherwise, pass nil to get the first page.
func (repo *UserRepository) GetAllUsers(
	ctx context.Context,
	pageSize int,
	nextToken string,
) ([]domain.User, string, error) {

	startKey, err := decodeStartKey(nextToken)
	if err != nil {
		return nil, "", fmt.Errorf("invalid pagination token: %w", err)
	}

	input := &dynamodb.ScanInput{
		TableName:         aws.String(repo.tableName),
		Limit:             aws.Int32(int32(pageSize)),
		ExclusiveStartKey: startKey,
	}

	result, err := repo.client.Scan(ctx, input)
	if err != nil {
		return nil, "", fmt.Errorf("failed to scan users: %w", err)
	}

	var users []domain.User
	if err := attributevalue.UnmarshalListOfMaps(result.Items, &users); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal users: %w", err)
	}

	nextTokenOut, err := encodeStartKey(result.LastEvaluatedKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to encode pagination token: %w", err)
	}

	return users, nextTokenOut, nil
}
