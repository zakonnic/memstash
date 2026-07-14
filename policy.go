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
	// PolicyWTinyLFU is W-TinyLFU adapted to lock-free reads: a small admission window in front of a GCLOCK main
	// queue, gated by a Count-Min frequency sketch that remembers keys across evictions. Strong on skewed
	// (Zipf-like) workloads where frequency beats recency.
	PolicyWTinyLFU
	// PolicySIEVE is SIEVE (NSDI'24): one insertion-ordered list with a hand that evicts unvisited items in place,
	// so retained items are never re-queued. Simpler than S3-FIFO with a comparable hit rate on web-like workloads.
	PolicySIEVE
)
