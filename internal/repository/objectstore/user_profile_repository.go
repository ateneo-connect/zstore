package objectstore

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// UserProfileRepository manages S3 interactions for user profiles.
type UserProfileRepository struct {
	client     *s3.Client
	bucketName string
}

// NewUserProfileRepository initializes a new UserProfileRepository.
func NewUserProfileRepository(client *s3.Client, bucketName string) UserProfileRepository {
	return UserProfileRepository{
		client:     client,
		bucketName: bucketName,
	}
}

// Upload uploads a user profile file to S3
func (r *UserProfileRepository) Upload(ctx context.Context, key string, reader io.Reader) error {
	_, err := r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})
	return err
}

// Download generates a pre-signed URL for accessing a user profile file from S3
func (r *UserProfileRepository) GetPresignedUrl(ctx context.Context, key string) (string, error) {
	presignClient := s3.NewPresignClient(r.client)

	request, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	return request.URL, nil
}

// Delete removes a user profile file from S3
func (r *UserProfileRepository) Delete(ctx context.Context, key string) error {
	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	return err
}
