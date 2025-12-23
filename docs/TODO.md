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
- [ ] **Auto-detect filename**: If destination filename is not specified, use the source filename and bucket root.
