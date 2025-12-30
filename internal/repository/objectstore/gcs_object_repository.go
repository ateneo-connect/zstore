package objectstore

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/transfermanager"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
)

// GCSObjectRepository implements ObjectRepository for Google Cloud Storage
type GCSObjectRepository struct {
	client     *storage.Client
	bucketName string
	downloader *transfermanager.Downloader
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
func (r *GCSObjectRepository) Download(ctx context.Context, key string, dest io.WriterAt, quiet bool) error {
	if !quiet {
		log.Debugf("Downloading from GCS: gs://%s/%s", r.bucketName, key)
	}

	// Get object attributes first to check size
	bucket := r.client.Bucket(r.bucketName)
	obj := bucket.Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get GCS object attributes: %w", err)
	}
	log.Debugf("GCS object %s size: %d bytes", key, attrs.Size)

	// Create reader for the object
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS reader: %w", err)
	}
	defer reader.Close()

	// Setup progress bar if not quiet
	var proxyReader io.Reader = reader
	if !quiet {
		bar := progressbar.DefaultBytes(attrs.Size, "downloading")
		pbReader := progressbar.NewReader(reader, bar)
		proxyReader = &pbReader
	}

	// Read all data with progress tracking
	data, err := io.ReadAll(proxyReader)
	if err != nil {
		return fmt.Errorf("failed to read from GCS: %w", err)
	}

	// Write to destination at offset 0
	_, err = dest.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to write to destination: %w", err)
	}

	log.Debugf("Completed GCS download for %s, wrote %d bytes", key, len(data))
	return nil
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
