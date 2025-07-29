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
	"github.com/golang-jwt/jwt/v5"
	"github.com/zzenonn/zstore/internal/integration"
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

	// Fetch the ECDSA keys from AWS Secret Manager
	err = config.loadECDSAKeys(cfg)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// loadECDSAKeys retrieves the ECDSA private and public keys from Secret Manager
func (c *Config) loadECDSAKeys(cfg aws.Config) error {
	secretManagerService, err := integration.NewAWSSSMService(cfg)
	if err != nil {
		return err
	}

	// Construct secret paths using environment variables
	privateKeySecretPath := getEnv("ECDSA_PRIVATE_KEY_SECRET_PATH", "/ecdsa/private-key")
	publicKeySecretPath := getEnv("ECDSA_PUBLIC_KEY_SECRET_PATH", "/ecdsa/public-key")

	// Fetch the ECDSA private key
	privateKey, err := secretManagerService.GetSecretValue(context.Background(), privateKeySecretPath)
	if err != nil {
		return err
	}

	ecdsaPrivateKey, err := jwt.ParseECPrivateKeyFromPEM([]byte(privateKey))
	c.ECDSAPrivateKey = ecdsaPrivateKey

	if err != nil {
		return err
	}

	// Fetch the ECDSA public key
	publicKey, err := secretManagerService.GetSecretValue(context.Background(), publicKeySecretPath)
	if err != nil {
		return err
	}

	ecdsaPublicKey, err := jwt.ParseECPublicKeyFromPEM([]byte(publicKey))

	if err != nil {
		return err
	}
	c.ECDSAPublicKey = ecdsaPublicKey

	return nil
}

// getEnv reads an environment variable or returns a default value if the variable is not set
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.ToLower(value)
	}
	return defaultValue
}
