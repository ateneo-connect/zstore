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

var uploadCmd = &cobra.Command{
	Use:   "upload [file-path] [zs://bucket/prefix/object]",
	Short: "Upload a file to S3",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		filePath, zsURL := args[0], args[1]
		
		// Parse zs:// URL to extract key
		if !strings.HasPrefix(zsURL, "zs://") {
			fmt.Printf("Error: URL must start with zs://\n")
			return
		}
		key := strings.TrimPrefix(zsURL, "zs://")
		// Keep bucket name as part of the prefix
		
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		quiet, _ := cmd.Flags().GetBool("quiet")
		dataShards, _ := cmd.Flags().GetInt("data-shards")
		parityShards, _ := cmd.Flags().GetInt("parity-shards")
		err = fileService.UploadFile(context.Background(), key, file, quiet, dataShards, parityShards)
		if err != nil {
			fmt.Printf("Error uploading file: %v\n", err)
			return
		}
		fmt.Printf("File uploaded successfully: %s -> %s\n", filePath, key)
	},
}

var downloadCmd = &cobra.Command{
	Use:   "download [zs://bucket/prefix/object] [output-path]",
	Short: "Download a file from S3",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		zsURL, outputPath := args[0], args[1]
		
		// Parse zs:// URL to extract key
		if !strings.HasPrefix(zsURL, "zs://") {
			fmt.Printf("Error: URL must start with zs://\n")
			return
		}
		key := strings.TrimPrefix(zsURL, "zs://")
		// Keep bucket name as part of the prefix
		
		quiet, _ := cmd.Flags().GetBool("quiet")
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

var deleteCmd = &cobra.Command{
	Use:   "delete [zs://bucket/prefix/object]",
	Short: "Delete a file from S3",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		zsURL := args[0]
		
		// Parse zs:// URL to extract key
		if !strings.HasPrefix(zsURL, "zs://") {
			fmt.Printf("Error: URL must start with zs://\n")
			return
		}
		key := strings.TrimPrefix(zsURL, "zs://")
		// Keep bucket name as part of the prefix
		
		err := fileService.DeleteFile(context.Background(), key)
		if err != nil {
			fmt.Printf("Error deleting file: %v\n", err)
			return
		}
		fmt.Printf("File deleted successfully: %s\n", key)
	},
}

func init() {
	uploadCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	uploadCmd.Flags().Int("data-shards", 4, "Number of data shards for erasure coding")
	uploadCmd.Flags().Int("parity-shards", 2, "Number of parity shards for erasure coding")
	downloadCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Suppress progress bars")
	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(deleteCmd)
}