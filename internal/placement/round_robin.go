package placement

import (
	"fmt"
	"sync"

	"github.com/zzenonn/zstore/internal/repository/objectstore"
)

// RoundRobinPlacer implements round-robin shard placement
type RoundRobinPlacer struct {
	mu           sync.RWMutex
	repositories map[string]objectstore.ObjectRepository
	bucketNames  []string
}

// NewRoundRobinPlacer creates a new round-robin placer
func NewRoundRobinPlacer() *RoundRobinPlacer {
	return &RoundRobinPlacer{
		repositories: make(map[string]objectstore.ObjectRepository),
		bucketNames:  make([]string, 0),
	}
}

// RegisterBucket adds a bucket and its repository
func (p *RoundRobinPlacer) RegisterBucket(bucketName string, repo objectstore.ObjectRepository) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if _, exists := p.repositories[bucketName]; exists {
		return fmt.Errorf("bucket %s already registered", bucketName)
	}

	p.repositories[bucketName] = repo
	p.bucketNames = append(p.bucketNames, bucketName)
	return nil
}

// GetRepositoryForBucket returns the repository for a specific bucket
func (p *RoundRobinPlacer) GetRepositoryForBucket(bucketName string) (objectstore.ObjectRepository, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	repo, exists := p.repositories[bucketName]
	if !exists {
		return nil, fmt.Errorf("no repository found for bucket: %s", bucketName)
	}
	return repo, nil
}

// Place selects a bucket using round-robin strategy
func (p *RoundRobinPlacer) Place(shardIndex int) (string, objectstore.ObjectRepository, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if len(p.bucketNames) == 0 {
		return "", nil, fmt.Errorf("no buckets registered")
	}

	bucketIndex := shardIndex % len(p.bucketNames)
	bucketName := p.bucketNames[bucketIndex]
	repo := p.repositories[bucketName]

	return bucketName, repo, nil
}

// ListBuckets returns all registered bucket names
func (p *RoundRobinPlacer) ListBuckets() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	buckets := make([]string, len(p.bucketNames))
	copy(buckets, p.bucketNames)
	return buckets
}
