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
)

// FetchingResourceError generates a formatted error for failed fetching of any resource by its type.
func FetchingResourceError(resource string) error {
	return fmt.Errorf("failed to fetch %s by id", resource)
}

func ConfigNotSetError(config string) error {
	return fmt.Errorf("The %s environment variable must be set", config)
}
