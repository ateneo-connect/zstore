# TODOs

## Architecture & Code Organization
- [x] **Separate RoundRobinPlacer into its own file** (`internal/placement/round_robin.go`)
- [x] **Remove obsolete storage_router.go** from service package
- [x] **Create config file logic** for persistent configuration management
- [x] **Implement list zs** command for listing stored files
- [x] **Add raw bucket selection flag** for specifying which bucket to use for `--no-erasure-coding` operations

## Testing & Quality
- [x] **Fix Tests:** Update benchmark tests to use placement.Placer instead of direct S3ObjectRepository
- [x] **Fix Tests:** Update integration tests to use new placement-based FileService constructor
- [x] **Fix Tests:** Update all tests to reflect new multi-provider architecture and placement system

## Performance & User Experience
- [x] **Make download concurrent** for improved performance with configurable concurrency
- [x] **Remove S3 references** from code comments and flags since the system now supports multiple providers
- [x] **Auto-detect filename**: If destination filename is not specified, use the source filename
- [x] **Add support for multiple storage prefixes**: Implemented s3:// support for raw operations via upload-raw/download-raw commands
- [ ] **Implement parallel range GET downloads** (Issue #4): Replace memory-based assembly with direct file writes using HTTP Range requests for both S3 and GCS
- [ ] **Add support for GCS and Azure**: Extend raw operations beyond S3 to support gs://, azure://, etc.

## ObjectRepository Interface Optimization
- [ ] **Change ObjectRepository.Download to return io.WriterAt instead of io.ReadCloser**:
  - Current interface forces memory allocation (ReadCloser -> buffer -> WriterAt)
  - Both S3 and GCS downloaders natively support WriterAt destinations
  - S3: `downloader.Download(ctx, writerAt, input)` - already supports WriterAt
  - GCS: `transfermanager.Downloader.DownloadObject(ctx, &DownloadObjectInput{Destination: writerAt})` - has WriterAt support
  - Benefits: Eliminates memory allocation, enables direct file writes, better performance
  - Impact: Breaking change to ObjectRepository interface, requires updating all callers
  - Implementation: Change `Download(ctx, key, quiet) (io.ReadCloser, error)` to `Download(ctx, key, dest io.WriterAt, quiet) error`

## Memory Management
- [ ] **Handle large files in S3 downloads**: Current pre-allocated buffer approach fails for files larger than available memory
  - Options: Size limit check, temp file fallback for large objects, or hybrid approach
  - Consider: Small files (<100MB) in memory, large files (>=100MB) to temp file or direct WriterAt

## GCS Transfer Manager Integration
- [ ] **Migrate GCS to use transfermanager package**: Replace direct storage client with transfermanager.Downloader
  - Use `transfermanager.NewDownloader(client)` and `DownloadObject()` with `DownloadBuffer` or direct WriterAt
  - Benefits: Parallel downloads, better performance, consistent with S3 approach
  - Note: transfermanager package is in preview but provides significant performance improvements
