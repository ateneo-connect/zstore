package objectstore

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
)

// GCSObjectRepository implements ObjectRepository for Google Cloud Storage
type GCSObjectRepository struct {
	client     *storage.Client
	bucketName string
}

// Upload uploads an object to GCS
func (r *GCSObjectRepository) Upload(ctx context.Context, key string, reader io.Reader, quiet bool) (string, error) {
	bucket := r.client.Bucket(r.bucketName)
	obj := bucket.Object(key)

	writer := obj.NewWriter(ctx)
	defer writer.Close()

	// Determine size for progress bar
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
		log.Debugf("Uploading to GCS: gs://%s/%s", r.bucketName, key)
		bar := progressbar.DefaultBytes(size, "uploading")
		pbReader := progressbar.NewReader(reader, bar)
		proxyReader = &pbReader
	}

	_, err := io.Copy(writer, proxyReader)
	if err != nil {
		return "", fmt.Errorf("failed to upload to GCS: %w", err)
	}

	return fmt.Sprintf("%s/%s", r.bucketName, key), nil
}

// progressReader wraps a ReadCloser with a progress bar
type progressReader struct {
	r   io.ReadCloser
	bar *progressbar.ProgressBar
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if pr.bar != nil {
		pr.bar.Add(n)
	}
	return n, err
}

func (pr *progressReader) Close() error {
	return pr.r.Close()
}

// Download downloads an object from GCS
func (r *GCSObjectRepository) Download(ctx context.Context, key string, quiet bool) (io.ReadCloser, error) {
	bucket := r.client.Bucket(r.bucketName)
	obj := bucket.Object(key)

	if !quiet {
		log.Debugf("Downloading from GCS: gs://%s/%s", r.bucketName, key)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to download from GCS: %w", err)
	}

	if quiet {
		return reader, nil
	}

	// Get object attributes for progress bar
	attrs, err := obj.Attrs(ctx)
	var bar *progressbar.ProgressBar
	if err == nil {
		bar = progressbar.DefaultBytes(attrs.Size, "downloading")
	}

	return &progressReader{r: reader, bar: bar}, nil
}

// Delete deletes an object from GCS
func (r *GCSObjectRepository) Delete(ctx context.Context, key string) error {
	bucket := r.client.Bucket(r.bucketName)
	obj := bucket.Object(key)

	err := obj.Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete from GCS: %w", err)
	}

	return nil
}

// DeletePrefix deletes all objects with the given prefix from GCS
func (r *GCSObjectRepository) DeletePrefix(ctx context.Context, prefix string) error {
	bucket := r.client.Bucket(r.bucketName)

	// List objects with prefix
	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == storage.ErrObjectNotExist {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects with prefix %s: %w", prefix, err)
		}

		// Delete each object
		obj := bucket.Object(attrs.Name)
		if err := obj.Delete(ctx); err != nil {
			log.Warnf("Failed to delete object %s: %v", attrs.Name, err)
		}
	}

	return nil
}

// GetBucketName returns the bucket name
func (r *GCSObjectRepository) GetBucketName() string {
	return r.bucketName
}

// GetStorageType returns the storage type
func (r *GCSObjectRepository) GetStorageType() string {
	return "gcs"
}
