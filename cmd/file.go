package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var quiet bool

// parseZsURL parses a zs:// URL and returns the key
func parseZsURL(zsURL string) (string, error) {
	if !strings.HasPrefix(zsURL, "zs://") {
		return "", fmt.Errorf("URL must start with zs://")
	}
	return strings.TrimPrefix(zsURL, "zs://"), nil
}

// parseS3URL parses an s3:// URL and returns the bucket and key
func parseS3URL(s3URL string) (string, string, error) {
	if !strings.HasPrefix(s3URL, "s3://") {
		return "", "", fmt.Errorf("URL must start with s3://")
	}
	path := strings.TrimPrefix(s3URL, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return parts[0], "", nil
	}
	return parts[0], parts[1], nil
}

// parseGCSURL parses a gs:// URL and returns the bucket and key
func parseGCSURL(gsURL string) (string, string, error) {
	if !strings.HasPrefix(gsURL, "gs://") {
		return "", "", fmt.Errorf("URL must start with gs://")
	}
	path := strings.TrimPrefix(gsURL, "gs://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return parts[0], "", nil
	}
	return parts[0], parts[1], nil
}

var uploadCmd = &cobra.Command{
	Use:   "upload [file-path] [zs://bucket/prefix/object]",
	Short: "Upload a file with erasure coding (destination optional - uses filename if not specified)",
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// Auto-detect destination if not provided or if destination ends with /
		var key string
		if len(args) == 2 {
			// Parse zs:// URL to extract key
			var err error
			key, err = parseZsURL(args[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
			// If key ends with /, append filename
			if strings.HasSuffix(key, "/") {
				key = key + filepath.Base(filePath)
			}
		} else {
			// Use source filename as destination key
			key = filepath.Base(filePath)
		}

		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		quiet, _ := cmd.Flags().GetBool("quiet")
		dataShards, _ := cmd.Flags().GetInt("data-shards")
		parityShards, _ := cmd.Flags().GetInt("parity-shards")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		err = fileService.UploadFile(context.Background(), key, file, quiet, dataShards, parityShards, concurrency)
		if err != nil {
			fmt.Printf("Error uploading file: %v\n", err)
			return
		}
		fmt.Printf("File uploaded successfully: %s -> %s\n", filePath, key)
	},
}

var uploadRawCmd = &cobra.Command{
	Use:   "upload-raw [file-path] [s3://bucket/object | gs://bucket/object]",
	Short: "Upload a file directly without erasure coding to S3 or GCS",
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		if len(args) < 2 {
			fmt.Printf("Error: destination URL is required (s3://bucket/key or gs://bucket/key)\n")
			return
		}
		
		url := args[1]
		var bucket, key string
		var err error
		
		// Parse URL based on scheme
		if strings.HasPrefix(url, "s3://") {
			bucket, key, err = parseS3URL(url)
		} else if strings.HasPrefix(url, "gs://") {
			bucket, key, err = parseGCSURL(url)
		} else {
			fmt.Printf("Error: URL must start with s3:// or gs://\n")
			return
		}
		
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		
		// If key ends with / or is empty, append filename
		if key == "" || strings.HasSuffix(key, "/") {
			key = key + filepath.Base(filePath)
		}

		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		quiet, _ := cmd.Flags().GetBool("quiet")
		
		// Route to appropriate repository
		if strings.HasPrefix(url, "s3://") {
			err = rawFileService.UploadToRepository(context.Background(), bucket, key, file, quiet)
			if err != nil {
				fmt.Printf("Error uploading to S3: %v\n", err)
				return
			}
			fmt.Printf("File uploaded successfully: %s -> s3://%s/%s\n", filePath, bucket, key)
		} else {
			err = rawFileService.UploadToRepository(context.Background(), bucket, key, file, quiet)
			if err != nil {
				fmt.Printf("Error uploading to GCS: %v\n", err)
				return
			}
			fmt.Printf("File uploaded successfully: %s -> gs://%s/%s\n", filePath, bucket, key)
		}
	},
}

var downloadCmd = &cobra.Command{
	Use:   "download [zs://bucket/prefix/object] [output-path]",
	Short: "Download a file with erasure coding reconstruction",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		zsURL, outputPath := args[0], args[1]

		// Parse zs:// URL to extract key
		key, err := parseZsURL(zsURL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		quiet, _ := cmd.Flags().GetBool("quiet")
		concurrency, _ := cmd.Flags().GetInt("concurrency")

		fileService.SetConcurrency(concurrency)
		reader, err := fileService.DownloadFile(context.Background(), key, quiet)
		if err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
			return
		}
		defer reader.Close()

		// If output path is a directory, use the filename from the key
		if stat, err := os.Stat(outputPath); err == nil && stat.IsDir() {
			fileName := filepath.Base(key)
			outputPath = filepath.Join(outputPath, fileName)
		}

		// Create output directory if it doesn't exist
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			fmt.Printf("Error creating output directory: %v\n", err)
			return
		}

		outFile, err := os.Create(outputPath)
		if err != nil {
			fmt.Printf("Error creating output file: %v\n", err)
			return
		}
		defer outFile.Close()

		_, err = io.Copy(outFile, reader)
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			return
		}

		fmt.Printf("File downloaded successfully: %s -> %s\n", key, outputPath)
	},
}

var downloadRawCmd = &cobra.Command{
	Use:   "download-raw [s3://bucket/object | gs://bucket/object] [output-path]",
	Short: "Download a file directly without erasure coding from S3 or GCS",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		url, outputPath := args[0], args[1]

		var bucket, key string
		var err error
		
		// Parse URL based on scheme
		if strings.HasPrefix(url, "s3://") {
			bucket, key, err = parseS3URL(url)
		} else if strings.HasPrefix(url, "gs://") {
			bucket, key, err = parseGCSURL(url)
		} else {
			fmt.Printf("Error: URL must start with s3:// or gs://\n")
			return
		}
		
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		quiet, _ := cmd.Flags().GetBool("quiet")
		
		// Route to appropriate repository
		var reader io.ReadCloser
		reader, err = rawFileService.DownloadFromRepository(context.Background(), bucket, key, quiet)
		
		if err != nil {
			fmt.Printf("Error downloading file: %v\n", err)
			return
		}
		defer reader.Close()

		// If output path is a directory, use the filename from the key
		if stat, err := os.Stat(outputPath); err == nil && stat.IsDir() {
			fileName := filepath.Base(key)
			outputPath = filepath.Join(outputPath, fileName)
		}

		// Create output directory if it doesn't exist
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			fmt.Printf("Error creating output directory: %v\n", err)
			return
		}

		outFile, err := os.Create(outputPath)
		if err != nil {
			fmt.Printf("Error creating output file: %v\n", err)
			return
		}
		defer outFile.Close()

		_, err = io.Copy(outFile, reader)
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			return
		}

		fmt.Printf("File downloaded successfully: %s -> %s\n", url, outputPath)
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete [zs://bucket/prefix/object]",
	Short: "Delete a file from cloud storage",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		zsURL := args[0]

		// Parse zs:// URL to extract key
		key, err := parseZsURL(zsURL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		err = fileService.DeleteFile(context.Background(), key)
		if err != nil {
			fmt.Printf("Error deleting file: %v\n", err)
			return
		}
		fmt.Printf("File deleted successfully: %s\n", key)
	},
}

var deleteRawCmd = &cobra.Command{
	Use:   "delete-raw [s3://bucket/object | gs://bucket/object]",
	Short: "Delete a file directly without erasure coding from S3 or GCS",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		url := args[0]

		var bucket, key string
		var err error
		
		// Parse URL based on scheme
		if strings.HasPrefix(url, "s3://") {
			bucket, key, err = parseS3URL(url)
		} else if strings.HasPrefix(url, "gs://") {
			bucket, key, err = parseGCSURL(url)
		} else {
			fmt.Printf("Error: URL must start with s3:// or gs://\n")
			return
		}
		
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		// Route to appropriate repository
		err = rawFileService.DeleteFromRepository(context.Background(), bucket, key)
		
		if err != nil {
			fmt.Printf("Error deleting file: %v\n", err)
			return
		}
		fmt.Printf("File deleted successfully: %s\n", url)
	},
}

var listCmd = &cobra.Command{
	Use:   "list [zs://bucket/prefix]",
	Short: "List files in cloud storage",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		zsURL := args[0]
		
		// Parse zs:// URL to extract prefix
		prefix, err := parseZsURL(zsURL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		
		// Remove trailing slash for consistent prefix matching
		prefix = strings.TrimSuffix(prefix, "/")
		
		files, err := fileService.ListFiles(context.Background(), prefix)
		if err != nil {
			fmt.Printf("Error listing files: %v\n", err)
			return
		}
		
		if len(files) == 0 {
			fmt.Printf("No files found in %s\n", zsURL)
			return
		}
		
		fmt.Printf("Files in %s:\n", zsURL)
		for _, file := range files {
			fmt.Printf("  %s/%s\n", file.Prefix, file.FileName)
		}
	},
}

func init() {
	uploadCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	uploadCmd.Flags().Int("data-shards", 4, "Number of data shards for erasure coding")
	uploadCmd.Flags().Int("parity-shards", 2, "Number of parity shards for erasure coding")
	uploadCmd.Flags().Int("concurrency", 3, "Number of concurrent shard uploads")
	uploadRawCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	downloadCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	downloadCmd.Flags().Int("concurrency", 3, "Number of concurrent shard downloads")
	downloadRawCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(uploadRawCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(downloadRawCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(deleteRawCmd)
	rootCmd.AddCommand(listCmd)
}
