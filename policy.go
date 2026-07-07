package memstash

// Policy selects the cache's eviction algorithm.
type Policy uint8

const (
	// PolicyS3FIFO uses three FIFO queues (small/main/ghost). The default policy:
	// it delivers the best hit rate on workloads with a large share of one-hit-wonder keys and scans.
	PolicyS3FIFO Policy = iota
	// PolicyClock is GCLOCK (second-chance with a 2-bit reference counter).
	// It approximates LRU at practically the cost of FIFO.
	PolicyClock
)
