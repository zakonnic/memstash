package memstash_test

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/zakonnic/memstash"
)

// A bounded in-process cache with no external dependencies.
func Example() {
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100_000), // weight units; without a cost function every item weighs 1
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "greeting", "hello")

	// GetFromMemory is the hottest read path: no context, no locks, no allocations.
	if v, ok := c.GetFromMemory("greeting"); ok {
		fmt.Println(v)
	}
	// Output: hello
}

// GetOrLoad resolves a miss with the loader and collapses concurrent misses on the same key into one call.
func ExampleCache_GetOrLoad() {
	c, _ := memstash.New[string, int](memstash.WithMemoryCapacity(1000))
	defer c.Close()

	ctx := context.Background()
	calls := 0
	load := func(ctx context.Context, key string) (int, error) {
		calls++
		return len(key), nil
	}

	for range 3 {
		v, err := c.GetOrLoad(ctx, "user:42", load)
		if err != nil {
			panic(err)
		}
		fmt.Print(v, " ")
	}
	fmt.Println("loader calls:", calls)
	// Output: 7 7 7 loader calls: 1
}

// NewLoadable fixes the loader at construction time, so GetOrLoad takes only a key.
func ExampleNewLoadable() {
	lc, err := memstash.NewLoadable(
		func(ctx context.Context, id int) (string, error) {
			if id < 0 {
				return "", errors.New("no such user")
			}
			return fmt.Sprintf("user-%d", id), nil
		},
		memstash.WithMemoryCapacity(1000),
	)
	if err != nil {
		panic(err)
	}
	defer lc.Close()

	name, _ := lc.GetOrLoad(context.Background(), 42)
	fmt.Println(name, lc.Len()) // Cache is embedded: Len comes from it
	// Output: user-42 1
}

// WithOnDeletion reports every item that leaves memory, with the reason it left.
func ExampleWithOnDeletion() {
	c, _ := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithOnDeletion(func(key, value string, cause memstash.DeletionCause) {
			fmt.Printf("%s=%s left: %s (automatic: %t)\n", key, value, cause, cause.Automatic())
		}),
	)
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "k", "v1")
	_ = c.Set(ctx, "k", "v2") // the old value is replaced
	_ = c.Delete(ctx, "k")
	// Output:
	// k=v1 left: replacement (automatic: false)
	// k=v2 left: invalidation (automatic: false)
}

// BatchGetFromMemory looks up many keys at once, appending the hits to a slice you own.
func ExampleCache_BatchGetFromMemory() {
	c, _ := memstash.New[string, int](memstash.WithMemoryCapacity(1000))
	defer c.Close()

	ctx := context.Background()
	for i, key := range []string{"a", "b", "c"} {
		_ = c.Set(ctx, key, i)
	}

	// Pass dst[:0] to reuse the backing array and keep the call allocation-free.
	dst := make(memstash.List[string, int], 0, 4)
	dst = c.BatchGetFromMemory([]string{"a", "c", "absent"}, dst)
	for _, kv := range dst {
		fmt.Printf("%s=%d ", kv.Key, kv.Value)
	}
	fmt.Println()
	// Output: a=0 c=2
}

// Iterator walks the live memory entries lock-free. The order is unspecified, so sort when it matters.
func ExampleCache_Iterator() {
	c, _ := memstash.New[string, int](memstash.WithMemoryCapacity(1000))
	defer c.Close()

	ctx := context.Background()
	for i, key := range []string{"a", "b", "c"} {
		_ = c.Set(ctx, key, i)
	}

	var keys []string
	for key, value := range c.Iterator() {
		keys = append(keys, fmt.Sprintf("%s=%d", key, value))
	}
	slices.Sort(keys)
	fmt.Println(keys)
	// Output: [a=0 b=1 c=2]
}
