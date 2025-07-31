package main

import (
	"context"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/logging"
	"github.com/zzenonn/zstore/internal/repository/db"
	"github.com/zzenonn/zstore/internal/repository/objectstore"
	"github.com/zzenonn/zstore/internal/service"
)

var (
	cfg         *config.Config
	userService *service.UserService
	fileService *service.FileService
)

var rootCmd = &cobra.Command{
	Use:   "app",
	Short: "CLI application for user and file management",
	Long:  "A CLI application built with Cobra for managing users and file operations",
}

func init() {
	cobra.OnInitialize(initConfig)
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize and migrate the database",
	Run: func(cmd *cobra.Command, args []string) {
		dynamoDb, err := db.NewDatabase(cfg)
		if err != nil {
			fmt.Printf("Failed to connect to the database: %v\n", err)
			return
		}

		if err := dynamoDb.MigrateDb(context.Background()); err != nil {
			fmt.Printf("Failed to migrate the database: %v\n", err)
			return
		}

		fmt.Println("Database initialized and migrated successfully")
	},
}

var downCmd = &cobra.Command{
	Use:   "down",
	Short: "Roll back database migrations",
	Run: func(cmd *cobra.Command, args []string) {
		dynamoDb, err := db.NewDatabase(cfg)
		if err != nil {
			fmt.Printf("Failed to connect to the database: %v\n", err)
			return
		}

		if err := dynamoDb.MigrateDown(context.Background()); err != nil {
			fmt.Printf("Failed to roll back migrations: %v\n", err)
			return
		}

		fmt.Println("Database migrations rolled back successfully")
	},
}

func initConfig() {
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	logging.InitLogger(cfg)

	// Initialize database connection only
	dynamoDb, err := db.NewDatabase(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	// Initialize services
	userRepository := db.NewUserRepository(dynamoDb.Client, cfg.DynamoDBTable)
	userService = service.NewUserService(&userRepository)

	s3Store := objectstore.NewS3ObjectStore(cfg)
	s3ObjectRepository := objectstore.NewS3ObjectRepository(s3Store.Client, cfg.S3BucketName)
	fileService = service.NewFileService(&s3ObjectRepository)
}

func init() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(downCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
