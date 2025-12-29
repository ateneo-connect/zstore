# Zstore

A multi-provider erasure coding object storage system that distributes file shards across multiple cloud storage backends (S3, GCS) for fault tolerance and performance.

## Quick Start

### 1. Configuration

Create a `config.yaml` file:

```yaml
# Zstore Configuration File
log_level: info
dynamodb_table: object_metadata

# Multi-bucket configuration
buckets:
  primary:
    bucket_name: my-gcs-bucket
    platform: gcs
  secondary:
    bucket_name: my-s3-bucket
    platform: s3
```

### 2. Environment Setup

```bash
# Set config file path
export ZSTORE_CONFIG_PATH=/path/to/your/config.yaml

# AWS credentials (for S3 and DynamoDB)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1

# GCS credentials (for GCS buckets)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### 3. Initialize Database

```bash
./zstore init
```

### 4. Basic Usage

```bash
# Upload a file with erasure coding (default: 4 data + 2 parity shards, concurrency 3)
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt

# Upload with auto-detected filename to specific prefix
./zstore upload /path/to/file.txt zs://my-bucket/folder/

# Upload with custom shard configuration
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt --data-shards 6 --parity-shards 3

# Upload without erasure coding (raw file)
./zstore upload-raw /path/to/file.txt s3://my-bucket/path/file.txt

# Upload raw file in quiet mode
./zstore upload-raw /path/to/file.txt s3://my-bucket/path/file.txt --quiet

# Upload in quiet mode (suppress progress bars)
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt --quiet

# Download a file (default concurrency 3)
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt

# Download with custom concurrency
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt --concurrency 5

# Download without erasure coding (raw file)
./zstore download-raw s3://my-bucket/path/file.txt /path/to/output.txt

# Download raw file in quiet mode
./zstore download-raw s3://my-bucket/path/file.txt /path/to/output.txt --quiet

# Download in quiet mode
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt --quiet

# List files in a bucket/prefix
./zstore list zs://my-bucket/path/

# Delete a file
./zstore delete zs://my-bucket/path/file.txt

# Delete a raw file
./zstore delete-raw s3://my-bucket/path/file.txt
```

## Command Options

### Global Options
- `--quiet, -q`: Suppress progress bars and verbose output

### Upload Options
- `--data-shards`: Number of data shards for erasure coding (default: 4)
- `--parity-shards`: Number of parity shards for erasure coding (default: 2)
- `--concurrency`: Number of concurrent shard uploads (default: 3)

### Download Options
- `--concurrency`: Number of concurrent shard downloads (default: 3)

### Raw Operations
- `upload-raw`: Upload files directly to S3 without erasure coding (uses s3:// URLs, supports --quiet)
- `download-raw`: Download files directly from S3 without erasure coding (uses s3:// URLs, supports --quiet)
- `delete-raw`: Delete files directly from S3 without erasure coding (uses s3:// URLs)

## Configuration

### Config File Format

The `config.yaml` file supports:

```yaml
# Logging level (debug, info, warn, error)
log_level: info

# DynamoDB table for metadata storage
dynamodb_table: object_metadata

# Storage buckets configuration
buckets:
  bucket_key_1:
    bucket_name: actual-bucket-name
    platform: s3  # or gcs
  bucket_key_2:
    bucket_name: another-bucket
    platform: gcs
```

### Supported Platforms

- **s3**: Amazon S3 buckets
- **gcs**: Google Cloud Storage buckets

### Multi-Provider Setup

Zstore automatically distributes shards across all configured buckets using round-robin placement:

- **Shard 0** → bucket_key_1
- **Shard 1** → bucket_key_2  
- **Shard 2** → bucket_key_1
- **Shard 3** → bucket_key_2
- etc.

## Features

### Erasure Coding
- **Reed-Solomon encoding** for fault tolerance
- **Configurable shards**: Choose data and parity shard counts
- **Automatic reconstruction** from available shards
- **Integrity verification** using CRC64 hashes

### Multi-Provider Storage
- **Cross-cloud distribution** (mix S3 and GCS)
- **Round-robin placement** for load balancing
- **Fault tolerance** across providers
- **Cost optimization** through provider diversity

### Performance
- **Concurrent uploads/downloads** with configurable concurrency
- **Dynamic concurrency control** for optimal performance
- **Early termination** when sufficient shards are available
- **Progress indicators** for large file operations

## Testing

### Running Tests

```bash
# Set config file for tests
export ZSTORE_CONFIG_PATH=/path/to/test-config.yaml

# Run integration tests
go test ./tests/service/

# Run benchmarks
go test -bench=. ./tests/service/
```

### Test Configuration

Tests require the `ZSTORE_CONFIG_PATH` environment variable to be set. Create a test-specific config file with test buckets.

## Architecture

### Components

- **Placement System**: Distributes shards across multiple storage backends
- **Erasure Coding Service**: Reed-Solomon encoding/decoding
- **Object Repositories**: S3 and GCS storage implementations
- **Metadata Repository**: DynamoDB for file reconstruction metadata
- **File Service**: High-level file operations with erasure coding
- **Raw File Service**: Direct storage operations without erasure coding

### Data Flow

1. **Upload**: File → Shards → Distribute across buckets → Store metadata
2. **Download**: Retrieve metadata → Download shards → Verify integrity → Reconstruct file
3. **Delete**: Remove shards from all buckets → Delete metadata

## Documentation

- [Architecture Diagrams](docs/): PlantUML diagrams showing system architecture
- [TODOs](docs/TODO.md): Development tasks and feature roadmap
- [Git Checkpoints](docs/GIT_CHECKPOINTS.md): Stable version tags and restore instructions
