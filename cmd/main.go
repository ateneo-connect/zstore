package main

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/logging"
	"github.com/zzenonn/zstore/internal/placement"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

var (
	cfg            *config.Config
	fileService    *service.FileService
	rawFileService *service.RawFileService
	configPath     string
)

var rootCmd = &cobra.Command{
	Use:   "zstore",
	Short: "CLI application for user and file management",
	Long:  "A CLI application built with Cobra for managing users and file operations",
}

func init() {
	cobra.OnInitialize(initConfig)
	setupFlags()
}

// setupFlags defines CLI flags
func setupFlags() {
	rootCmd.PersistentFlags().StringVar(&configPath, "config", "", "config file path (default is ./config.yaml)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("dynamodb-table", "default-table", "DynamoDB table name")
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize and migrate the database",
	Run:   runInitCommand,
}

var debugCmd = &cobra.Command{
	Use:   "debug",
	Short: "Show configuration for debugging",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Configuration:\n")
		fmt.Printf("  Log Level: %s\n", cfg.LogLevel)
		fmt.Printf("  DynamoDB Table: %s\n", cfg.DynamoDBTable)
		fmt.Printf("  DynamoDB Region: %s\n", cfg.DynamoDBRegion)
		fmt.Printf("\nBuckets:\n")
		for key, bucket := range cfg.Buckets {
			fmt.Printf("  %s:\n", key)
			fmt.Printf("    Name: %s\n", bucket.BucketName)
			fmt.Printf("    Platform: %s\n", bucket.Platform)
			fmt.Printf("    Region: %s\n", bucket.Region)
		}
	},
}

var downCmd = &cobra.Command{
	Use:   "down",
	Short: "Roll back database migrations",
	Run:   runDownCommand,
}

// runInitCommand handles database initialization
func runInitCommand(cmd *cobra.Command, args []string) {
	dynamoDb, err := db.NewDatabase(cfg.AwsConfig)
	if err != nil {
		fmt.Printf("Failed to connect to the database: %v\n", err)
		return
	}

	if err := dynamoDb.MigrateDb(context.Background()); err != nil {
		fmt.Printf("Failed to migrate the database: %v\n", err)
		return
	}

	fmt.Println("Database initialized and migrated successfully")
}

// runDownCommand handles database migration rollback
func runDownCommand(cmd *cobra.Command, args []string) {
	dynamoDb, err := db.NewDatabase(cfg.AwsConfig)
	if err != nil {
		fmt.Printf("Failed to connect to the database: %v\n", err)
		return
	}

	if err := dynamoDb.MigrateDown(context.Background()); err != nil {
		fmt.Printf("Failed to roll back migrations: %v\n", err)
		return
	}

	fmt.Println("Database migrations rolled back successfully")
}

func initConfig() {
	var err error
	cfg, err = config.LoadConfig(configPath, rootCmd)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	logging.InitLogger(cfg)

	dynamoDb, err := db.NewDatabase(cfg.AwsConfig)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	placer := initRepositories(cfg.AwsConfig, cfg.GcsClient, cfg.Buckets)
	metadataRepository := db.NewMetadataRepository(dynamoDb.Client, cfg.DynamoDBTable)

	// Create repository factory for raw file service
	factory := objectstore.NewObjectRepositoryFactory(cfg.AwsConfig, cfg.GcsClient)

	fileService = service.NewFileService(placer, &metadataRepository)
	rawFileService = service.NewRawFileService(factory)
}

// initRepositories initializes the placement system and repositories
func initRepositories(awsConfig aws.Config, gcsClient *storage.Client, buckets map[string]config.BucketConfig) placement.Placer {
	// Create factory that can build S3 and GCS repositories
	factory := objectstore.NewObjectRepositoryFactory(awsConfig, gcsClient)
	// Create round-robin placer for distributing shards across buckets
	placer := placement.NewRoundRobinPlacer()

	// Register each configured bucket with the placer
	for bucketKey, bucketConfig := range buckets {
		repo := createRepository(factory, bucketKey, bucketConfig)
		if repo != nil {
			// Add repository to placement system
			placer.RegisterBucket(bucketKey, repo)
		}
	}

	return placer
}

// createRepository creates a single repository from bucket configuration
func createRepository(factory *objectstore.ObjectRepositoryFactory, bucketKey string, bucketConfig config.BucketConfig) objectstore.ObjectRepository {
	// Convert config format to factory format
	repoConfig := objectstore.BucketConfig{
		Name:   bucketConfig.BucketName,
		Type:   objectstore.RepositoryType(bucketConfig.Platform), // "s3" or "gcs"
		Region: bucketConfig.Region,
	}

	// Use factory to create appropriate repository (S3 or GCS)
	repo, err := factory.CreateRepository(repoConfig)
	if err != nil {
		// Log warning but continue with other buckets
		log.Warnf("Failed to create repository for bucket %s: %v", bucketKey, err)
		return nil
	}

	return repo
}

func init() {
	addCommands()
}

// addCommands registers subcommands
func addCommands() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(downCmd)
	rootCmd.AddCommand(debugCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
