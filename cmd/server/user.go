package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zzenonn/zstore/internal/domain"
)

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "User management commands",
	Long:  "CRUD operations for user management",
}

var userCreateCmd = &cobra.Command{
	Use:   "create [username] [password]",
	Short: "Create a new user",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, password := args[0], args[1]
		user := domain.User{
			Username: &username,
			Password: password,
		}

		createdUser, err := userService.CreateUser(context.Background(), user)
		if err != nil {
			fmt.Printf("Error creating user: %v\n", err)
			return
		}
		fmt.Printf("User created successfully: %s\n", *createdUser.Username)
	},
}

var userReadCmd = &cobra.Command{
	Use:   "read [username]",
	Short: "Read user information",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		username := args[0]
		user, err := userService.GetUser(context.Background(), username)
		if err != nil {
			fmt.Printf("Error reading user: %v\n", err)
			return
		}
		fmt.Printf("Username: %s\n", *user.Username)
	},
}

var userUpdateCmd = &cobra.Command{
	Use:   "update [username] [new-password]",
	Short: "Update user password",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, newPassword := args[0], args[1]
		user := domain.User{
			Username: &username,
			Password: newPassword,
		}

		updatedUser, err := userService.UpdateUser(context.Background(), user)
		if err != nil {
			fmt.Printf("Error updating user: %v\n", err)
			return
		}
		fmt.Printf("User updated successfully: %s\n", *updatedUser.Username)
	},
}

var userDeleteCmd = &cobra.Command{
	Use:   "delete [username]",
	Short: "Delete a user",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		username := args[0]
		err := userService.DeleteUser(context.Background(), username)
		if err != nil {
			fmt.Printf("Error deleting user: %v\n", err)
			return
		}
		fmt.Printf("User deleted successfully: %s\n", username)
	},
}

var userLoginCmd = &cobra.Command{
	Use:   "login [username] [password]",
	Short: "Login user",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		username, password := args[0], args[1]
		err := userService.Login(context.Background(), username, password)
		if err != nil {
			fmt.Printf("Login failed: %v\n", err)
			return
		}
		fmt.Printf("Login successful for user: %s\n", username)
	},
}

func init() {
	userCmd.AddCommand(userCreateCmd)
	userCmd.AddCommand(userReadCmd)
	userCmd.AddCommand(userUpdateCmd)
	userCmd.AddCommand(userDeleteCmd)
	userCmd.AddCommand(userLoginCmd)
	rootCmd.AddCommand(userCmd)
}