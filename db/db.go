package db

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

type FileDB struct {
	counter Counter
	mu      sync.Mutex
}

func NewFileDB() *FileDB {
	return &FileDB{
		counter: Counter{},
	}
}

type Counter struct {
	countA int32
	countB int32
}

// increaseA increments countA in a thread-safe manner using atomic operations
func (c *Counter) increaseA(i int32) {
	atomic.AddInt32(&c.countA, i)
}

// increaseB increments countB in a thread-safe manner using atomic operations
func (c *Counter) increaseB(i int32) {
	atomic.AddInt32(&c.countB, i)
}

// Save saves the counter values to a file named "file.txt"
func (db *FileDB) Save() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	file, err := os.Create("file.txt")
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "Count A: %d\nCount B: %d", db.counter.countA, db.counter.countB)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}
