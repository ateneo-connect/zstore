package config

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zzenonn/zstore/internal/errors"
)

// BucketConfig represents a storage bucket configuration
type BucketConfig struct {
	BucketName string `yaml:"bucket_name"`
	Platform   string `yaml:"platform"`
}

// Config holds the application configuration
type Config struct {
	LogLevel        string `yaml:"log_level"`
	ECDSAPrivateKey *ecdsa.PrivateKey
	ECDSAPublicKey  *ecdsa.PublicKey
	// AwsConfig: AWS SDK uses a shared configuration object that contains
	// credentials, region, retry policies, etc. Multiple AWS services
	// (S3, DynamoDB, etc.) are created from this single config.
	AwsConfig aws.Config
	// AwsRegion: Explicitly configured AWS region
	AwsRegion       string `yaml:"aws_region"`
	// GcsClient: Google Cloud SDK uses individual service clients that
	// handle their own configuration internally via environment variables,
	// service account files, or metadata service. No shared config needed.
	GcsClient       *storage.Client
	DynamoDBTable   string                  `yaml:"dynamodb_table"`
	Buckets         map[string]BucketConfig `yaml:"buckets"`
}

// LoadConfig loads configuration from config.yaml, environment variables, or CLI flags
// Priority: CLI flags > Environment variables > config.yaml > defaults
func LoadConfig(configPath string, rootCmd *cobra.Command) (*Config, error) {
	// Enable automatic environment variable reading first
	viper.AutomaticEnv()
	
	// Check for ZSTORE_CONFIG_PATH environment variable if no config path provided
	if configPath == "" {
		if envPath := viper.GetString("ZSTORE_CONFIG_PATH"); envPath != "" {
			configPath = envPath
		}
	}
	
	if err := setupViper(configPath, rootCmd); err != nil {
		return nil, err
	}

	awsConfig, awsRegion, err := loadAWSConfig()
	if err != nil {
		return nil, err
	}

	gcsClient, err := loadGCSClient()
	if err != nil {
		return nil, err
	}

	buckets := parseBuckets()

	return &Config{
		LogLevel:      viper.GetString("log_level"),
		AwsConfig:     awsConfig,
		AwsRegion:     awsRegion,
		GcsClient:     gcsClient,
		DynamoDBTable: viper.GetString("dynamodb_table"),
		Buckets:       buckets,
	}, nil
}

// setupViper configures Viper with defaults, paths, and bindings
func setupViper(configPath string, rootCmd *cobra.Command) error {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	if configPath != "" {
		viper.SetConfigFile(configPath)
	}

	setDefaults()
	viper.AutomaticEnv()

	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		return fmt.Errorf("failed to bind flags: %w", err)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("error reading config file: %w", err)
		}
	}

	return nil
}

// setDefaults sets default configuration values
func setDefaults() {
	viper.SetDefault("log_level", "info")
	viper.SetDefault("dynamodb_table", "default-table")
	viper.SetDefault("buckets", map[string]interface{}{
		"default-bucket": map[string]interface{}{
			"bucket_name": "default-bucket",
			"platform":    "s3",
		},
	})
}

// loadAWSConfig loads AWS SDK configuration with explicit region handling
func loadAWSConfig() (aws.Config, string, error) {
	// Priority order for region configuration:
	// 1. config.yaml: aws_region
	// 2. Environment: AWS_REGION
	// 3. Environment: AWS_DEFAULT_REGION
	// 4. Error if none found
	
	region := viper.GetString("aws_region")
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		return aws.Config{}, "", errors.ErrAWSRegionNotConfigured
	}
	
	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return aws.Config{}, "", fmt.Errorf("unable to load AWS SDK config: %v", err)
	}
	return cfg, region, nil
}

// loadGCSClient loads Google Cloud Storage client
func loadGCSClient() (*storage.Client, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to create GCS client: %v", err)
	}
	return client, nil
}

// parseBuckets parses bucket configuration from Viper
func parseBuckets() map[string]BucketConfig {
	bucketsMap := make(map[string]BucketConfig)
	bucketsRaw := viper.GetStringMap("buckets")

	for key, value := range bucketsRaw {
		if bucketMap, ok := value.(map[string]interface{}); ok {
			bucketsMap[key] = BucketConfig{
				BucketName: getString(bucketMap, "bucket_name", key),
				Platform:   getString(bucketMap, "platform", "s3"),
			}
		}
	}

	return bucketsMap
}

// SetConfigValue sets a configuration value (used for CLI flags)
func SetConfigValue(key string, value interface{}) {
	viper.Set(key, value)
}

// getString safely extracts string value from map with default
func getString(m map[string]interface{}, key, defaultValue string) string {
	if value, exists := m[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return defaultValue
}
