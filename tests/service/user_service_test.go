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

// MockUserRepository is a mock implementation of the UserRepository interface
type MockUserRepository struct {
	mock.Mock
}

func (m *MockUserRepository) CreateUser(ctx context.Context, user domain.User) (domain.User, error) {
	args := m.Called(ctx, user)
	return args.Get(0).(domain.User), args.Error(1)
}

func (m *MockUserRepository) GetUser(ctx context.Context, username string) (domain.User, error) {
	args := m.Called(ctx, username)
	return args.Get(0).(domain.User), args.Error(1)
}

func (m *MockUserRepository) UpdateUser(ctx context.Context, id string, user domain.User) (domain.User, error) {
	args := m.Called(ctx, id, user)
	return args.Get(0).(domain.User), args.Error(1)
}

func (m *MockUserRepository) DeleteUser(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockUserRepository) GetAllUsers(ctx context.Context,
	pageSize int,
	nextToken string,
) ([]domain.User, string, error) {
	args := m.Called(ctx, pageSize, nextToken)
	return args.Get(0).([]domain.User), args.Get(1).(string), args.Error(2)
}

// TestCreateUser tests the CreateUser method of the UserService
func TestCreateUser(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	user := domain.User{
		Username:       stringPtr("testuser"),
		HashedPassword: []byte("hashedpassword"),
	}

	// Mock the GetUser method to return an empty user (indicating the username does not exist)
	mockRepo.On("GetUser", ctx, *user.Username).Return(domain.User{}, errors.ErrInvalidUser)

	// Mock the CreateUser method to return the inserted user
	mockRepo.On("CreateUser", ctx, mock.AnythingOfType("domain.User")).Return(user, nil)

	createdUser, err := userService.CreateUser(ctx, user)

	assert.NoError(t, err)
	assert.Equal(t, *user.Username, *createdUser.Username)

	mockRepo.AssertExpectations(t)
}

// TestCreateUser_UsernameExists tests the CreateUser method when the username already exists
func TestCreateUser_UsernameExists(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	existingUser := domain.User{
		Username: stringPtr("testuser"),
	}

	// Mock the GetUser method to return an existing user (indicating the username already exists)
	mockRepo.On("GetUser", ctx, *existingUser.Username).Return(existingUser, nil)

	user := domain.User{
		Username: stringPtr("testuser"),
	}

	_, err := userService.CreateUser(ctx, user)

	assert.Error(t, err)
	assert.Equal(t, errors.ErrInvalidUser, err)

	mockRepo.AssertExpectations(t)
}

// TestGetUser tests the GetUser method of the UserService
func TestGetUser(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	expectedUser := domain.User{
		Username: stringPtr("testuser"),
	}

	// Mock the GetUser method to return the expected user
	mockRepo.On("GetUser", ctx, *expectedUser.Username).Return(expectedUser, nil)

	user, err := userService.GetUser(ctx, *expectedUser.Username)

	assert.NoError(t, err)
	assert.Equal(t, expectedUser, user)

	mockRepo.AssertExpectations(t)
}

// TestLogin tests the Login method of the UserService
func TestLogin(t *testing.T) {
	mockRepo := new(MockUserRepository)
	userService := service.NewUserService(mockRepo)

	ctx := context.Background()
	expectedUser := domain.User{
		Username:       stringPtr("testuser"),
		HashedPassword: []byte("$2a$10$hPSU2uhX6jABfAf63G5MmeqZnpMCA.1mZcD1f5WP757Bk67vu5boq"), // bcrypt hash of "password"
	}

	// Mock the GetUser method to return the expected user
	mockRepo.On("GetUser", ctx, *expectedUser.Username).Return(expectedUser, nil)

	// Test with correct password
	err := userService.Login(ctx, *expectedUser.Username, "password")
	assert.NoError(t, err)

	// Test with incorrect password
	err = userService.Login(ctx, *expectedUser.Username, "wrongpassword")
	assert.Error(t, err)
	assert.Equal(t, errors.ErrInvalidUser, err)

	mockRepo.AssertExpectations(t)
}

// Helper function to return a pointer to a string
func stringPtr(s string) *string {
	return &s
}
