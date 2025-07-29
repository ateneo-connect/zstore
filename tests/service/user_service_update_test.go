package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/errors"
	"github.com/zzenonn/zstore/internal/service"
)

// TestUpdateUser_Success tests the UpdateUser method when the update is successful
func TestUpdateUser_Success(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	existingUser := domain.User{
		Username: stringPtr("testuser"),
	}

	updatedUser := domain.User{
		Username: stringPtr("testuser"),
		Password: "newpassword",
	}

	// Mock the GetUser method to return the existing user
	mockRepo.On("GetUser", ctx, *existingUser.Username).Return(existingUser, nil)

	// Mock the UpdateUser method to return the updated user
	mockRepo.On("UpdateUser", ctx, existingUser.Username, mock.AnythingOfType("domain.User")).Return(updatedUser, nil)

	result, err := userService.UpdateUser(ctx, updatedUser)

	assert.NoError(t, err)
	assert.Equal(t, *updatedUser.Username, *result.Username)

	mockRepo.AssertExpectations(t)
}

// TestUpdateUser_UserNotFound tests the UpdateUser method when the user does not exist
func TestUpdateUser_UserNotFound(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	nonExistentUser := domain.User{
		Username: stringPtr("nonexistentuser"),
	}

	// Mock the GetUser method to return an error (user not found)
	mockRepo.On("GetUser", ctx, *nonExistentUser.Username).Return(domain.User{}, errors.ErrInvalidUser)

	_, err := userService.UpdateUser(ctx, nonExistentUser)

	assert.Error(t, err)
	assert.Equal(t, errors.ErrInvalidUser, err)

	mockRepo.AssertExpectations(t)
}

// TestUpdateUser_PasswordUpdate tests the UpdateUser method when the user updates their password
func TestUpdateUser_PasswordUpdate(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	existingUser := domain.User{
		Username: stringPtr("testuser"),
	}

	updatedUser := domain.User{
		Username: stringPtr("testuser"),
		Password: "newpassword",
	}

	// Mock the GetUser method to return the existing user
	mockRepo.On("GetUser", ctx, *existingUser.Username).Return(existingUser, nil)

	// Mock the UpdateUser method to return the updated user
	mockRepo.On("UpdateUser", ctx, existingUser.Username, mock.AnythingOfType("domain.User")).Return(updatedUser, nil)

	result, err := userService.UpdateUser(ctx, updatedUser)

	assert.NoError(t, err)
	assert.Equal(t, *updatedUser.Username, *result.Username)

	mockRepo.AssertExpectations(t)
}

// TestUpdateUser_ErrorDuringUpdate tests the UpdateUser method when there is an error during the update process
func TestUpdateUser_ErrorDuringUpdate(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	existingUser := domain.User{
		Username: stringPtr("testuser"),
	}

	updatedUser := domain.User{
		Username: stringPtr("testuser"),
		Password: "newpassword",
	}

	// Mock the GetUser method to return the existing user
	mockRepo.On("GetUser", ctx, *existingUser.Username).Return(existingUser, nil)

	// Mock the UpdateUser method to return an error
	mockRepo.On("UpdateUser", ctx, existingUser.Username, mock.AnythingOfType("domain.User")).Return(domain.User{}, errors.ErrInvalidUser)

	_, err := userService.UpdateUser(ctx, updatedUser)

	assert.Error(t, err)
	assert.Equal(t, errors.ErrInvalidUser, err)

	mockRepo.AssertExpectations(t)
}
