package errors

import (
	"errors"
	"fmt"
)

var (
	ErrNotImplemented        = errors.New("this function is not yet implemented")
	ErrInvalidUser           = errors.New("invalid username or password")
	ErrMissingRequiredFields = errors.New("missing required fields")
	ErrInsufficientShards    = errors.New("insufficient shards available for reconstruction")
	ErrEmptyFile             = errors.New("cannot upload empty file")
	ErrFileIntegrityCheck    = errors.New("file integrity check failed")
	ErrAWSRegionNotConfigured = errors.New(`DynamoDB region not configured. Please set region using one of:
1. config.yaml: dynamodb_region: us-east-1
2. Environment: export AWS_REGION=us-east-1
3. Environment: export AWS_DEFAULT_REGION=us-east-1

Common regions: us-east-1, us-west-2, eu-west-1, ap-southeast-1`)
)

// FetchingResourceError generates a formatted error for failed fetching of any resource by its type.
func FetchingResourceError(resource string) error {
	return fmt.Errorf("failed to fetch %s by id", resource)
}

func ConfigNotSetError(config string) error {
	return fmt.Errorf("The %s environment variable must be set", config)
}
