// Package dynamo_adapter adapts Amazon DynamoDB (github.com/aws/aws-sdk-go-v2/service/dynamodb) to the
// memstash.L2Cache contract. DynamoDB is a good managed L2 in AWS deployments: it scales horizontally and expires
// items on its own via a TTL attribute.
//
// The constructor takes the DynamoAPI interface below, satisfied by *dynamodb.Client. Users of the higher-level
// github.com/guregu/dynamo wrapper, which is built on the same SDK client, can pass their underlying *dynamodb.Client.
//
// Table convention: a partition key named "cache_key" (String); the value is stored under "value" (Binary) and the
// expiry under "ttl" (Number, unix seconds). Enable DynamoDB TTL on the "ttl" attribute. Because DynamoDB's TTL
// sweep is delayed (up to ~48h), the adapter also treats an item whose "ttl" is already in the past as absent.
package dynamo_adapter

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// errUnprocessed is returned when DynamoDB keeps handing back UnprocessedKeys/UnprocessedItems past the retry budget.
var errUnprocessed = errors.New("memstash/l2/dynamo_adapter: batch not fully processed after retries")

const (
	keyAttr   = "cache_key"
	valueAttr = "value"
	ttlAttr   = "ttl"

	batchGetLimit   = 100 // DynamoDB BatchGetItem hard limit
	batchWriteLimit = 25  // DynamoDB BatchWriteItem hard limit
	maxBatchRetries = 4   // bounded retries for UnprocessedKeys / UnprocessedItems
)

// DynamoAPI is the subset of the DynamoDB SDK the adapter needs; *dynamodb.Client satisfies it.
type DynamoAPI interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// Cache is an L2 adapter over a DynamoDB table. The client's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	client  DynamoAPI
	table   string
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. table is the DynamoDB table name (required). By default keys
// must be strings (identity mapping); for other key types pass l2.WithKeyFunc.
func New[K comparable, V any](client DynamoAPI, codec memstash.Codec[V], table string, opts ...memstash.Option) (*Cache[K, V], error) {
	if client == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{client: client, table: table, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](client DynamoAPI, table string, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](client, l2.JSONCodec[V](), table, opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](client DynamoAPI, table string, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](client, l2.BytesCodec(), table, opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](client DynamoAPI, codec memstash.Codec[V], table string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](client, codec, table, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewCacheJSON builds a two-level cache with the JSON value codec (see NewCache).
func NewCacheJSON[K comparable, V any](client DynamoAPI, table string, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](client, l2.JSONCodec[V](), table, opts...)
}

// NewCacheBytes builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewCacheBytes[K comparable](client DynamoAPI, table string, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](client, l2.BytesCodec(), table, opts...)
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	out, err := c.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.table),
		Key:       map[string]types.AttributeValue{keyAttr: &types.AttributeValueMemberS{Value: c.keyFunc(key)}},
	})
	if err != nil {
		return zero, false, err
	}
	value, ok, err := c.decodeItem(out.Item)
	if err != nil || !ok {
		return zero, false, err
	}
	return value, true, nil
}

// Set stores the value; ttl == 0 means "no expiration".
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	item, err := c.encodeItem(c.keyFunc(key), value, ttl)
	if err != nil {
		return err
	}
	_, err = c.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(c.table), Item: item})
	return err
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	_, err := c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.table),
		Key:       map[string]types.AttributeValue{keyAttr: &types.AttributeValueMemberS{Value: c.keyFunc(key)}},
	})
	return err
}

// BatchGet fetches the keys with BatchGetItem in chunks of 100, retrying UnprocessedKeys a bounded number of times.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	// DynamoDB returns items unordered and keyed only by their storage string, so map each storage key back to the
	// caller's K (which may not be a string).
	byStorageKey := make(map[string]K, len(keys))
	for _, key := range keys {
		byStorageKey[c.keyFunc(key)] = key
	}
	for start := 0; start < len(keys); start += batchGetLimit {
		end := min(start+batchGetLimit, len(keys))

		req := make([]map[string]types.AttributeValue, 0, end-start)
		for _, key := range keys[start:end] {
			req = append(req, map[string]types.AttributeValue{keyAttr: &types.AttributeValueMemberS{Value: c.keyFunc(key)}})
		}
		pending := map[string]types.KeysAndAttributes{c.table: {Keys: req}}

		for attempt := 0; len(pending) > 0; attempt++ {
			if attempt > maxBatchRetries {
				return found, errUnprocessed
			}
			out, err := c.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: pending})
			if err != nil {
				return found, err
			}
			for _, item := range out.Responses[c.table] {
				storageKey, ok := item[keyAttr].(*types.AttributeValueMemberS)
				if !ok {
					continue
				}
				key, known := byStorageKey[storageKey.Value]
				if !known {
					continue
				}
				value, present, err := c.decodeItem(item)
				if err != nil {
					return found, err
				}
				if present {
					found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
				}
			}
			pending = out.UnprocessedKeys
		}
	}
	return found, nil
}

// BatchSet stores the items with BatchWriteItem in chunks of 25, retrying UnprocessedItems a bounded number of times.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	for start := 0; start < len(items); start += batchWriteLimit {
		end := min(start+batchWriteLimit, len(items))

		writes := make([]types.WriteRequest, 0, end-start)
		for _, item := range items[start:end] {
			encoded, err := c.encodeItem(c.keyFunc(item.Key), item.Value, ttl)
			if err != nil {
				return err
			}
			writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: encoded}})
		}
		pending := map[string][]types.WriteRequest{c.table: writes}

		for attempt := 0; len(pending) > 0; attempt++ {
			if attempt > maxBatchRetries {
				return errUnprocessed
			}
			out, err := c.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return err
			}
			pending = out.UnprocessedItems
		}
	}
	return nil
}

func (c *Cache[K, V]) encodeItem(storageKey string, value V, ttl time.Duration) (map[string]types.AttributeValue, error) {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return nil, err
	}
	item := map[string]types.AttributeValue{
		keyAttr:   &types.AttributeValueMemberS{Value: storageKey},
		valueAttr: &types.AttributeValueMemberB{Value: data},
	}
	if ttl > 0 {
		item[ttlAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)}
	}
	return item, nil
}

// decodeItem reads and decodes an item, reporting ok == false for a missing item or one whose TTL has already passed.
func (c *Cache[K, V]) decodeItem(item map[string]types.AttributeValue) (V, bool, error) {
	var zero V
	if item == nil || expired(item) {
		return zero, false, nil
	}
	raw, ok := item[valueAttr].(*types.AttributeValueMemberB)
	if !ok {
		return zero, false, nil
	}
	value, err := c.codec.Unmarshal(raw.Value)
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func expired(item map[string]types.AttributeValue) bool {
	av, ok := item[ttlAttr].(*types.AttributeValueMemberN)
	if !ok {
		return false
	}
	deadline, err := strconv.ParseInt(av.Value, 10, 64)
	return err == nil && deadline != 0 && deadline <= time.Now().Unix()
}
