package config

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// Config holds the application configuration
type Config struct {
	LogLevel        string
	Port            int
	ECDSAPrivateKey *ecdsa.PrivateKey
	ECDSAPublicKey  *ecdsa.PublicKey
	AwsConfig       aws.Config
	DynamoDBTable   string
	S3BucketName    string
}

// LoadConfig loads the configuration from environment variables and fetches the ECDSA keys from Secret Manager
func LoadConfig() (*Config, error) {
	// Load PORT, with a default of 8080 if the environment variable is not set
	portStr := getEnv("PORT", "8080")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Invalid value for PORT: %v", err)
		return nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %v", err)
	}

	config := &Config{
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		Port:          port,
		AwsConfig:     cfg,
		DynamoDBTable: getEnv("DYNAMODB_TABLE", "default-table"),
		S3BucketName:  getEnv("S3_BUCKET_NAME", "default-bucket"),
	}

	return config, nil
}

// getEnv reads an environment variable or returns a default value if the variable is not set
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.ToLower(value)
	}
	return defaultValue
}
