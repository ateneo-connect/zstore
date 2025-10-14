# Zstore

## Git Checkpoints

### Stable Checkpoint: v0.1.0-stable
This tag marks a stable version with separate upload/download functions before experimental work.

**To restore to this checkpoint:**
```bash
git checkout v0.1.0-stable
```

**To create a new branch from this tag:**
```bash
git checkout -b restore-stable v0.1.0-stable
```

**To see tag details:**
```bash
git show v0.1.0-stable
```

## TODOs

### Architecture & Code Organization
- [x] **Separate RoundRobinPlacer into its own file** (`internal/placement/round_robin.go`)
- [x] **Remove obsolete storage_router.go** from service package
- [ ] **Create config file logic** for persistent configuration management
- [ ] **Implement list zs** command for listing stored files
- [ ] **Add raw bucket selection flag** for specifying which bucket to use for `--no-erasure-coding` operations

### Testing & Quality
- [ ] **Fix Tests:** Update benchmark tests to use placement.Placer instead of direct S3ObjectRepository
- [ ] **Fix Tests:** Update integration tests to use new placement-based FileService constructor
- [ ] **Fix Tests:** Update all tests to reflect new multi-provider architecture and placement system

### Performance & User Experience
- [ ] **Make download concurrent** for improved performance with configurable concurrency
- [ ] **Remove S3 references** from code comments and flags since the system now supports multiple providers
