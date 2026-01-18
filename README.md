# Zstore

A multi-provider erasure coding object storage system that distributes file shards across multiple cloud storage backends (S3, GCS) for fault tolerance and performance.

## Quick Start

### 1. Configuration

Create a `config.yaml` file:

```yaml
# Zstore Configuration File
log_level: info
dynamodb_table: object_metadata

# DynamoDB region (required)
# Can also be set via AWS_REGION or AWS_DEFAULT_REGION environment variables
dynamodb_region: us-east-1

# Multi-bucket configuration with per-bucket regions
buckets:
  primary:
    bucket_name: my-gcs-bucket
    platform: gcs
    # region not needed for GCS
  secondary:
    bucket_name: my-s3-bucket
    platform: s3
    region: us-west-2  # Required for S3 buckets
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

#### Upload Commands

**Upload with Erasure Coding (default: 4 data + 2 parity shards, concurrency 3)**
```bash
# Upload a file with erasure coding
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt

# Upload with auto-detected filename to specific prefix
./zstore upload /path/to/file.txt zs://my-bucket/folder/

# Upload with custom shard configuration
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt --data-shards 6 --parity-shards 3

# Upload in quiet mode (suppress progress bars)
./zstore upload /path/to/file.txt zs://my-bucket/path/file.txt --quiet
```

**Upload Raw Files (without erasure coding)**
```bash
# Upload without erasure coding (raw file) - region required for S3
./zstore upload-raw /path/to/file.txt s3://my-bucket/path/file.txt --region us-west-2

# Upload raw file to GCS (no region needed)
./zstore upload-raw /path/to/file.txt gs://my-bucket/path/file.txt

# Upload raw file in quiet mode
./zstore upload-raw /path/to/file.txt s3://my-bucket/path/file.txt --region us-west-2 --quiet
```

#### Download Commands

**Download with Erasure Coding (default concurrency: 3)**
```bash
# Download a file with erasure coding reconstruction
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt

# Download with custom concurrency
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt --concurrency 5

# Download in quiet mode
./zstore download zs://my-bucket/path/file.txt /path/to/output.txt --quiet
```

**Download Raw Files (without erasure coding)**
```bash
# Download without erasure coding (raw file) - region required for S3
./zstore download-raw s3://my-bucket/path/file.txt /path/to/output.txt --region us-west-2

# Download raw file from GCS (no region needed)
./zstore download-raw gs://my-bucket/path/file.txt /path/to/output.txt

# Download raw file in quiet mode
./zstore download-raw s3://my-bucket/path/file.txt /path/to/output.txt --region us-west-2 --quiet
```

#### Delete Commands

**Delete with Erasure Coding**
```bash
# Delete a file (removes all shards and metadata)
./zstore delete zs://my-bucket/path/file.txt
```

**Delete Raw Files**
```bash
# Delete a raw file from S3 - region required
./zstore delete-raw s3://my-bucket/path/file.txt --region us-west-2

# Delete a raw file from GCS (no region needed)
./zstore delete-raw gs://my-bucket/path/file.txt
```

#### List Commands

```bash
# List files in a bucket/prefix
./zstore list zs://my-bucket/path/
```

## Command Options

### Global Options
- `--quiet, -q`: Suppress progress bars and verbose output
- `--config`: Config file path (default: ./config.yaml)
- `--log-level`: Log level - debug, info, warn, error (default: info)
- `--dynamodb-table`: DynamoDB table name (default: default-table)

### Upload Options
- `--data-shards`: Number of data shards for erasure coding (default: 4)
- `--parity-shards`: Number of parity shards for erasure coding (default: 2)
- `--concurrency`: Number of concurrent shard uploads (default: 3)

### Download Options
- `--concurrency`: Number of concurrent shard downloads (default: 3)
- `--verify-integrity`: Enable CRC64 hash verification of downloaded shards (default: false)

### Raw Operations
- `upload-raw`: Upload files directly to S3/GCS without erasure coding (uses s3:// or gs:// URLs, --region required for S3)
- `download-raw`: Download files directly from S3/GCS without erasure coding (uses s3:// or gs:// URLs, --region required for S3)
- `delete-raw`: Delete files directly from S3/GCS without erasure coding (uses s3:// or gs:// URLs, --region required for S3)

## Configuration

### Config File Format

The `config.yaml` file supports:

```yaml
# Logging level (debug, info, warn, error)
log_level: info

# DynamoDB region (required)
# Priority: config.yaml > AWS_REGION env > AWS_DEFAULT_REGION env
dynamodb_region: us-east-1

# DynamoDB table for metadata storage
dynamodb_table: object_metadata

# Storage buckets configuration
buckets:
  bucket_key_1:
    bucket_name: actual-bucket-name
    platform: s3
    region: us-west-2  # Required for S3 buckets
  bucket_key_2:
    bucket_name: another-bucket
    platform: gcs
    # region not needed for GCS
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

### Benchmarks

Zstore includes comprehensive benchmarks to measure performance across different scenarios:

```bash
# Run all benchmarks (both erasure-coded and raw operations)
go test -bench=. "-run=^$" ./tests/service/

# Run only erasure-coded benchmarks
go test -bench=BenchmarkFileService_ErasureCoded "-run=^$" ./tests/service/

# Run only raw operation benchmarks
go test -bench=BenchmarkRawFileService "-run=^$" ./tests/service/

# Run specific benchmark categories
go test -bench=BenchmarkFileService_ErasureCoded_UploadFile "-run=^$" ./tests/service/
go test -bench=BenchmarkRawFileService_UploadFile "-run=^$" ./tests/service/
go test -bench=BenchmarkRawFileService_CrossProvider_Comparison "-run=^$" ./tests/service/
```

**Benchmark Categories:**
- **Erasure-Coded Operations**: Tests file operations with Reed-Solomon encoding across multiple buckets
  - `BenchmarkFileService_ErasureCoded_UploadFile`: Upload performance across file sizes (1KB to 10MB)
  - `BenchmarkFileService_ErasureCoded_DownloadFile`: Download performance with shard reconstruction
  - `BenchmarkFileService_ErasureCoded_ConcurrencyComparison`: Impact of concurrency levels (1-5)
- **Raw Operations**: Direct storage operations without erasure coding
  - `BenchmarkRawFileService_UploadFile`: Direct uploads to S3/GCS buckets by provider
  - `BenchmarkRawFileService_DownloadFile`: Direct downloads from S3/GCS buckets by provider
  - `BenchmarkRawFileService_CrossProvider_Comparison`: Performance comparison between S3 and GCS

**Benchmark Results Format:**
- **Erasure-coded**: `BenchmarkFileService_ErasureCoded_UploadFile/1KB-16`
- **Raw S3**: `BenchmarkRawFileService_UploadFile/s3_primary/1KB-16`
- **Raw GCS**: `BenchmarkRawFileService_UploadFile/gcs_secondary/1KB-16`
- **Cross-provider**: `BenchmarkRawFileService_CrossProvider_Comparison/Raw_s3_primary-16`

**Metrics Measured:**
- **Throughput**: MB/s for upload/download operations
- **Latency**: Time per operation (GET, PUT, DELETE)
- **Memory Usage**: Allocation patterns and peak memory consumption
- **Concurrency Scaling**: Performance gains from parallel operations

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