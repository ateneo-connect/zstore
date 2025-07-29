package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	rgTypes "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/repository/migrate"
)

func init() {
	// Set log level based on environment variables
	switch logLevel := strings.ToLower(os.Getenv("LOG_LEVEL")); logLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}
}

type Migration interface {
	Up(ctx context.Context, client *dynamodb.Client) error
	Down(ctx context.Context, client *dynamodb.Client) error
	Version() string
	TableName() string
}

func (d *DynamoDb) MigrateDb(ctx context.Context) error {
	log.Info("migrating database")

	// Define migrations in order
	migrations := []Migration{
		&migrate.CreateUsersTable{}, // Add more migrations here
	}
	for _, migration := range migrations {
		// Check if migration was already applied
		if applied, err := d.isMigrationApplied(ctx, migration.Version()); err != nil {
			return fmt.Errorf("could not check migration status: %w", err)
		} else if applied {
			log.Infof("skipping migration %s: already applied", migration.Version())
			continue
		}

		// Apply migration
		if err := migration.Up(ctx, d.Client); err != nil {
			return fmt.Errorf("could not apply migration %s: %w", migration.Version(), err)
		}

		// Record migration using tags
		if err := d.recordMigration(ctx, migration.Version(), migration.TableName()); err != nil {
			return fmt.Errorf("could not record migration %s: %w", migration.Version(), err)
		}

		log.Infof("successfully applied migration %s", migration.Version())
	}

	log.Info("successfully migrated the database")
	return nil
}

func (d *DynamoDb) isMigrationApplied(ctx context.Context, version string) (bool, error) {
	input := &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []rgTypes.TagFilter{
			{
				Key:    aws.String("Migration"),
				Values: []string{version},
			},
		},
		ResourceTypeFilters: []string{"dynamodb:table"},
	}

	result, err := d.TaggingClient.GetResources(ctx, input)
	if err != nil {
		return false, fmt.Errorf("failed to check migration tags: %w", err)
	}

	return len(result.ResourceTagMappingList) > 0, nil
}

func (d *DynamoDb) recordMigration(ctx context.Context, version string, tableName string) error {
	// Get table ARN
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	tableDesc, err := d.Client.DescribeTable(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to get table ARN: %w", err)
	}

	// Tag the table
	input := &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{*tableDesc.Table.TableArn},
		Tags: map[string]string{
			"Migration":  version,
			"MigratedAt": time.Now().UTC().Format(time.RFC3339),
		},
	}

	_, err = d.TaggingClient.TagResources(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to tag resource: %w", err)
	}

	return nil
}
