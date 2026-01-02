# Performance Improvements

This document outlines potential performance optimizations for the Zstore erasure coding system.

## Current Performance Issues

### Streaming vs Memory-Based Processing

The current streaming implementation using temp files is significantly slower than the original memory-based version due to:

1. **Excessive Disk I/O**
   - Upload: File → Temp shards → Upload (2x disk writes)
   - Download: Download → Temp shards → Reconstruct → Final file (2x disk writes)
   - Original: File → Memory → Upload (1x disk read, no temp files)

2. **Sequential Processing**
   - Sharding must complete before uploads start
   - Reconstruction must download all shards before reconstruction begins
   - Original could process in memory immediately

3. **File System Overhead**
   - Creating/deleting temp files
   - File system metadata operations
   - Disk sync operations

## Proposed Solutions

### Option 1: Hybrid Approach (Recommended)

Implement size-based processing strategy:

```go
// Use memory for small files, streaming for large files
const MEMORY_THRESHOLD = 100 * 1024 * 1024 // 100MB

if fileSize < MEMORY_THRESHOLD {
    return shardFileInMemory(reader, dataShards, parityShards, fileSize)
} else {
    return ShardFile(reader, dataShards, parityShards, fileSize) // Current streaming
}
```

**Benefits:**
- Fast processing for typical file sizes
- Memory safety for large files
- Backward compatible

**Implementation:**
- Add `shardFileInMemory()` function using original memory-based approach
- Add size threshold configuration option
- Automatic fallback to streaming for large files

### Option 2: Pipeline Processing

Overlap sharding and uploading operations:

```go
// Start uploading shards as soon as they're created
// Don't wait for all shards to complete before starting uploads
// Use channels to coordinate between sharding and uploading goroutines
```

**Benefits:**
- Reduced total processing time
- Better resource utilization
- Maintains streaming benefits

**Implementation:**
- Refactor upload process to use producer-consumer pattern
- Create shard upload queue with goroutine workers
- Start uploads immediately when shards are ready

### Option 3: RAM Disk Integration

Use RAM disk for temp files when available:

```go
// Priority order for temp file location:
// 1. /dev/shm (Linux RAM disk)
// 2. User-specified ZSTORE_TEMP_DIR
// 3. Current working directory
// 4. System temp directory
```

**Benefits:**
- Memory-like speed with streaming safety
- Automatic fallback to disk if RAM insufficient
- No code changes to core logic

**Implementation:**
- Add temp directory detection logic
- Environment variable configuration
- Graceful fallback chain

### Option 4: Configurable Temp Location

Allow users to specify high-performance storage:

```bash
# Environment variables
export ZSTORE_TEMP_DIR=/mnt/nvme-ssd/temp
export ZSTORE_MEMORY_THRESHOLD=50MB
export ZSTORE_USE_RAMDISK=true
```

**Benefits:**
- User control over performance vs memory trade-offs
- Support for various storage configurations
- Easy performance tuning

**Implementation:**
- Configuration system integration
- Runtime temp directory selection
- Performance monitoring and recommendations

## Implementation Priority

1. **Phase 1: Hybrid Approach**
   - Implement memory-based processing for small files
   - Add configurable size threshold
   - Maintain current streaming for large files

2. **Phase 2: Temp Directory Optimization**
   - Add RAM disk detection and usage
   - Implement configurable temp directory
   - Add environment variable support

3. **Phase 3: Pipeline Processing**
   - Refactor for concurrent sharding and uploading
   - Implement producer-consumer pattern
   - Add performance metrics and monitoring

## Performance Metrics to Track

- **Upload throughput** (MB/s) by file size
- **Memory usage** during operations
- **Temp disk usage** and cleanup efficiency
- **Time breakdown**: sharding vs uploading vs cleanup
- **Concurrency effectiveness** (actual vs theoretical speedup)

## Configuration Options

```yaml
# Future config.yaml additions
performance:
  memory_threshold: 100MB      # Switch to streaming above this size
  temp_directory: ""           # Custom temp dir (empty = auto-detect)
  use_ramdisk: true           # Prefer RAM disk when available
  pipeline_uploads: true      # Enable concurrent sharding/uploading
  max_memory_usage: 1GB       # Memory limit for in-memory processing
```

## Benchmarking Plan

1. **Baseline Measurements**
   - Current streaming performance across file sizes
   - Memory usage patterns
   - Disk I/O patterns

2. **Hybrid Implementation Testing**
   - Performance comparison at various thresholds
   - Memory usage validation
   - Edge case testing (threshold boundary conditions)

3. **Production Validation**
   - Real-world workload testing
   - Long-running stability tests
   - Resource usage monitoring

## Risk Assessment

### Hybrid Approach Risks
- **Memory exhaustion** if threshold too high
- **Complexity** in maintaining two code paths
- **Testing overhead** for both implementations

### Mitigation Strategies
- Conservative default thresholds
- Comprehensive memory monitoring
- Automated fallback mechanisms
- Extensive test coverage for both paths

## Future Considerations

- **Compression integration** during sharding
- **GPU acceleration** for Reed-Solomon operations
- **Network-aware sharding** (upload while sharding)
- **Adaptive thresholds** based on available system resources