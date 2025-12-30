package objectstore

import (
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
// TODO: Handle large files that exceed available memory. Current implementation
// pre-allocates entire file size in memory which will fail for very large objects.
// Consider: size limit check, temp file fallback, or hybrid approach (small files in memory, large files to temp file)
func (r *S3ObjectRepository) Download(ctx context.Context, key string, dest io.WriterAt, quiet bool) error {
	downloader := manager.NewDownloader(r.client)
	
	// Add progress bar if not quiet
	var writer io.WriterAt = dest
	if !quiet {
		headResult, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(r.bucketName),
			Key:    aws.String(key),
		})
		if err == nil && headResult.ContentLength != nil {
			bar := progressbar.DefaultBytes(*headResult.ContentLength, "downloading")
			writer = &progressWriterAt{w: dest, bar: bar}
		}
	}
	
	_, err := downloader.Download(ctx, writer, &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName),
		Key:    aws.String(key),
	})
	return err
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
