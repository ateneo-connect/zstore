package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var fileCmd = &cobra.Command{
	Use:   "file",
	Short: "File management commands",
	Long:  "CRUD operations for file management",
}

var fileCreateCmd = &cobra.Command{
	Use:   "create [username] [file-path]",
	Short: "Create/upload a profile file for a user",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, filePath := args[0], args[1]
		
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		key := filepath.Base(filePath)
		err = userProfileService.UploadProfile(context.Background(), username, key, file)
		if err != nil {
			fmt.Printf("Error creating file: %v\n", err)
			return
		}
		fmt.Printf("File created successfully for user %s: %s\n", username, key)
	},
}

var fileReadCmd = &cobra.Command{
	Use:   "read [username] [filename]",
	Short: "Read/download a user's profile file (generates presigned URL)",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, filename := args[0], args[1]
		
		url, err := userProfileService.GeneratePresignedURL(context.Background(), username, filename)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			return
		}
		fmt.Printf("Download URL for %s/%s:\n%s\n", username, filename, url)
	},
}

var fileUpdateCmd = &cobra.Command{
	Use:   "update [username] [file-path]",
	Short: "Update/replace a profile file for a user",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, filePath := args[0], args[1]
		
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			return
		}
		defer file.Close()

		key := filepath.Base(filePath)
		err = userProfileService.UploadProfile(context.Background(), username, key, file)
		if err != nil {
			fmt.Printf("Error updating file: %v\n", err)
			return
		}
		fmt.Printf("File updated successfully for user %s: %s\n", username, key)
	},
}

var fileDeleteCmd = &cobra.Command{
	Use:   "delete [username] [filename]",
	Short: "Delete a user's profile file",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, filename := args[0], args[1]
		
		err := userProfileService.DeleteProfile(context.Background(), username, filename)
		if err != nil {
			fmt.Printf("Error deleting file: %v\n", err)
			return
		}
		fmt.Printf("File deleted successfully: %s/%s\n", username, filename)
	},
}

func init() {
	fileCmd.AddCommand(fileCreateCmd)
	fileCmd.AddCommand(fileReadCmd)
	fileCmd.AddCommand(fileUpdateCmd)
	fileCmd.AddCommand(fileDeleteCmd)
	rootCmd.AddCommand(fileCmd)
}