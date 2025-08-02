package objectstore

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v3"
)

// S3ObjectRepository manages S3 interactions for objects.
type S3ObjectRepository struct {
	client     *s3.Client
	bucketName string
}

// NewS3ObjectRepository initializes a new S3ObjectRepository.
func NewS3ObjectRepository(client *s3.Client, bucketName string) S3ObjectRepository {
	return S3ObjectRepository{
		client:     client,
		bucketName: bucketName,
	}
}

// GetBucketName returns the bucket name.
func (r *S3ObjectRepository) GetBucketName() string {
	return r.bucketName
}

// GetStorageType returns the object store type.
func (r *S3ObjectRepository) GetStorageType() string {
	return "s3"
}

// Upload uploads an object file to S3
func (r *S3ObjectRepository) Upload(ctx context.Context, key string, reader io.Reader, quiet bool) (string, error) {
	seeker, ok := reader.(io.Seeker)
	var size int64 = -1
	if ok {
		if current, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			if end, err := seeker.Seek(0, io.SeekEnd); err == nil {
				size = end - current
				seeker.Seek(current, io.SeekStart)
			}
		}
	}

	var proxyReader io.Reader = reader
	if !quiet {
		bar := progressbar.DefaultBytes(size, "uploading")
		pbReader := progressbar.NewReader(reader, bar)
		proxyReader = &pbReader
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
		Body:   proxyReader,
	}
	if size > 0 {
		input.ContentLength = &size
	}

	_, err := r.client.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	return r.bucketName + "/" + key, nil
}

// Download downloads an object file from S3
func (r *S3ObjectRepository) Download(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	result, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	size := result.ContentLength
	if !quiet {
		bar := progressbar.DefaultBytes(*size, "downloading")
		proxyReader := progressbar.NewReader(result.Body, bar)
		return &progressReaderCloser{Reader: &proxyReader, Closer: result.Body}, nil
	}
	return result.Body, nil
}

type progressReaderCloser struct {
	io.Reader
	io.Closer
}

// Delete removes an object file from S3
func (r *S3ObjectRepository) Delete(ctx context.Context, key string) error {
	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	return err
}

// DeletePrefix removes all objects with the given prefix from S3
func (r *S3ObjectRepository) DeletePrefix(ctx context.Context, prefix string) error {
	// List objects with the prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(prefix),
	}

	for {
		result, err := r.client.ListObjectsV2(ctx, listInput)
		if err != nil {
			return err
		}

		// Delete objects using existing Delete function
		for _, obj := range result.Contents {
			if err := r.Delete(ctx, *obj.Key); err != nil {
				return err
			}
		}

		// Check if there are more objects to delete
		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		listInput.ContinuationToken = result.NextContinuationToken
	}

	return nil
}
