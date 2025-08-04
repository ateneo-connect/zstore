package objectstore

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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
	uploader := manager.NewUploader(r.client)
	
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

	_, err := uploader.Upload(ctx, input)
	if err != nil {
		return "", err
	}
	return r.bucketName + "/" + key, nil
}

// progressWriterAt wraps a WriterAt with a progress bar
type progressWriterAt struct {
	w   io.WriterAt
	bar *progressbar.ProgressBar
}

func (pw *progressWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = pw.w.WriteAt(p, off)
	if pw.bar != nil {
		pw.bar.Add(n)
	}
	return n, err
}

// Download downloads an object file from S3
func (r *S3ObjectRepository) Download(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	downloader := manager.NewDownloader(r.client)
	
	// Get object info for progress bar
	var bar *progressbar.ProgressBar
	if !quiet {
		headResult, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(r.bucketName),
			Key:    aws.String(key),
		})
		if err == nil && headResult.ContentLength != nil {
			bar = progressbar.DefaultBytes(*headResult.ContentLength, "downloading")
		}
	}
	
	// Create a buffer to download into
	buf := manager.NewWriteAtBuffer([]byte{})
	writer := &progressWriterAt{w: buf, bar: bar}
	
	input := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	}
	
	_, err := downloader.Download(ctx, writer, input)
	if err != nil {
		return nil, err
	}
	
	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
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
