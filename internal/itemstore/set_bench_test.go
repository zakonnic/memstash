package itemstore

import (
	"fmt"
	"testing"
)

func benchDifferenceKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

func BenchmarkSetDifference(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			keys := benchDifferenceKeys(n)
			hits := keys[:n/2] // half the batch answered by L2, the usual shape
			b.ReportAllocs()
			for b.Loop() {
				var buf [128]SetSlot[string]
				found := NewSetFrom(buf[:])
				for _, key := range hits {
					found.Add(key)
				}
				missing := 0
				for _, key := range keys {
					if !found.Exists(key) {
						missing++
					}
				}
				if missing != n-len(hits) {
					b.Fatalf("got %d missing, want %d", missing, n-len(hits))
				}
			}
		})
	}
}

func BenchmarkMapDifference(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			keys := benchDifferenceKeys(n)
			hits := keys[:n/2]
			b.ReportAllocs()
			for b.Loop() {
				found := make(map[string]struct{}, len(hits))
				for _, key := range hits {
					found[key] = struct{}{}
				}
				missing := 0
				for _, key := range keys {
					if _, ok := found[key]; !ok {
						missing++
					}
				}
				if missing != n-len(hits) {
					b.Fatalf("got %d missing, want %d", missing, n-len(hits))
				}
			}
		})
	}
}
