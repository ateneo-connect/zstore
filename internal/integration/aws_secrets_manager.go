package integration

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

type SecretsManagerService interface {
	GetSecretValue(ctx context.Context, secretName string) (string, error)
}

type AWSSSMService struct {
	client *ssm.Client
}

func NewAWSSSMService(cfg aws.Config) (*AWSSSMService, error) {
	client := ssm.NewFromConfig(cfg)

	return &AWSSSMService{
		client: client,
	}, nil
}

func (p *AWSSSMService) GetSecretValue(ctx context.Context, name string) (string, error) {
	input := &ssm.GetParameterInput{
		Name:           &name,
		WithDecryption: aws.Bool(true),
	}

	result, err := p.client.GetParameter(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get parameter from SSM: %v", err)
	}

	if result.Parameter == nil || result.Parameter.Value == nil {
		return "", fmt.Errorf("parameter value is nil")
	}

	return *result.Parameter.Value, nil
}
