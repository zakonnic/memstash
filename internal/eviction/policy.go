// Package eviction implements the shard's eviction policies (Clock, S3-FIFO, W-TinyLFU and SIEVE) on top of the
// itemstate primitives. Every method runs strictly under the owning shard's mutex; queue nodes carry pool indices
// resolved through the shard's Pool handed in at construction.
//
// The policy contract itself is public: memstash.EvictionPolicy. The types here implement it, and custom policies
// plugged in through memstash.WithCustomEvictionPolicy implement the same interface.
package eviction
