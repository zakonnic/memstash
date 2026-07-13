// Package mongo_adapter adapts a MongoDB collection (go.mongodb.org/mongo-driver) to the memstash.L2Cache contract.
// It is a reasonable L2 when a service already runs MongoDB: documents are keyed by _id and expire through a TTL
// index on the expireAt field.
//
// The constructor takes a *mongo.Collection. Call EnsureTTLIndex once at startup so MongoDB removes expired
// documents on its own. Because the TTL monitor runs only about once a minute, the adapter also treats a document
// whose expireAt is already in the past as absent.
package mongo_adapter

import (
	"context"
	"errors"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// doc is the stored document: the storage key as _id, the encoded value as Binary, and an optional expiry date.
type doc struct {
	ID       string    `bson:"_id"`
	Value    []byte    `bson:"v"`
	ExpireAt time.Time `bson:"expireAt,omitempty"`
}

// Cache is an L2 adapter over a MongoDB collection. The collection's lifecycle stays with the caller.
type Cache[K comparable, V any] struct {
	coll    *mongo.Collection
	codec   memstash.Codec[V]
	keyFunc func(K) string
}

var _ memstash.L2Cache[string, string] = (*Cache[string, string])(nil)

// New creates the adapter with an explicit value codec. By default keys must be strings (identity mapping); for other
// key types pass l2.WithKeyFunc.
func New[K comparable, V any](coll *mongo.Collection, codec memstash.Codec[V], opts ...memstash.Option) (*Cache[K, V], error) {
	if coll == nil {
		return nil, l2.ErrNilClient
	}
	if codec == nil {
		return nil, l2.ErrNilCodec
	}
	keyFunc, err := l2.ResolveOptions[K](opts)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{coll: coll, codec: codec, keyFunc: keyFunc}, nil
}

// NewJSON creates the adapter that marshals values with encoding/json.
func NewJSON[K comparable, V any](coll *mongo.Collection, opts ...memstash.Option) (*Cache[K, V], error) {
	return New[K, V](coll, l2.JSONCodec[V](), opts...)
}

// NewBytes creates the adapter that passes []byte values through unchanged.
func NewBytes[K comparable](coll *mongo.Collection, opts ...memstash.Option) (*Cache[K, []byte], error) {
	return New[K, []byte](coll, l2.BytesCodec(), opts...)
}

// NewCache builds a two-level cache in one call: a new memstash.Cache backed by this adapter as its L2. The single
// option list is shared: the memstash.With* options configure the cache, l2.WithKeyFunc configures the adapter.
func NewCache[K comparable, V any](coll *mongo.Collection, codec memstash.Codec[V], opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	adapter, err := New[K, V](coll, codec, opts...)
	if err != nil {
		return nil, err
	}
	cacheOpts := make([]memstash.Option, 0, len(opts)+1)
	cacheOpts = append(cacheOpts, opts...)
	cacheOpts = append(cacheOpts, memstash.WithL2Cache[K, V](adapter))
	return memstash.New[K, V](cacheOpts...)
}

// NewJSONCache builds a two-level cache with the JSON value codec (see NewCache).
func NewJSONCache[K comparable, V any](coll *mongo.Collection, opts ...memstash.Option) (*memstash.Cache[K, V], error) {
	return NewCache[K, V](coll, l2.JSONCodec[V](), opts...)
}

// NewBytesCache builds a two-level cache that passes []byte values through unchanged (see NewCache).
func NewBytesCache[K comparable](coll *mongo.Collection, opts ...memstash.Option) (*memstash.Cache[K, []byte], error) {
	return NewCache[K, []byte](coll, l2.BytesCodec(), opts...)
}

// NewStringCache builds a two-level cache that passes string values through unchanged (see NewCache).
func NewStringCache[K comparable](coll *mongo.Collection, opts ...memstash.Option) (*memstash.Cache[K, string], error) {
	return NewCache[K, string](coll, l2.StringCodec(), opts...)
}

// EnsureTTLIndex creates the TTL index on expireAt so MongoDB deletes expired documents. Safe to call repeatedly.
func (c *Cache[K, V]) EnsureTTLIndex(ctx context.Context) error {
	_, err := c.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	return err
}

// Get returns the value; a missing (or expired) key is (zero, false, nil).
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	var d doc
	if err := c.coll.FindOne(ctx, bson.M{"_id": c.keyFunc(key)}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return zero, false, nil
		}
		return zero, false, err
	}
	if isExpired(d) {
		return zero, false, nil
	}
	value, err := c.codec.Unmarshal(d.Value)
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

// Set stores the value; ttl == 0 means "no expiration".
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	update, err := c.updateDoc(value, ttl)
	if err != nil {
		return err
	}
	_, err = c.coll.UpdateOne(ctx, bson.M{"_id": c.keyFunc(key)}, update, options.Update().SetUpsert(true))
	return err
}

// Delete removes the key; a missing key is not an error.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	_, err := c.coll.DeleteOne(ctx, bson.M{"_id": c.keyFunc(key)})
	return err
}

// BatchGet fetches all keys with a single {_id: {$in: [...]}} query.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (memstash.List[K, V], error) {
	found := make(memstash.List[K, V], 0, len(keys))
	if len(keys) == 0 {
		return found, nil
	}
	storageKeys := make([]string, len(keys))
	byStorageKey := make(map[string]K, len(keys))
	for i, key := range keys {
		storageKeys[i] = c.keyFunc(key)
		byStorageKey[storageKeys[i]] = key
	}

	cursor, err := c.coll.Find(ctx, bson.M{"_id": bson.M{"$in": storageKeys}})
	if err != nil {
		return found, err
	}
	var docs []doc
	if err := cursor.All(ctx, &docs); err != nil {
		return found, err
	}
	for i := range docs {
		if isExpired(docs[i]) {
			continue
		}
		key, ok := byStorageKey[docs[i].ID]
		if !ok {
			continue
		}
		value, err := c.codec.Unmarshal(docs[i].Value)
		if err != nil {
			return found, err
		}
		found = append(found, memstash.KeyVal[K, V]{Key: key, Value: value})
	}
	return found, nil
}

// BatchSet stores all items in one unordered BulkWrite of upserts; duplicate keys collapse to the last value.
// ttl == 0 means "no expiration".
func (c *Cache[K, V]) BatchSet(ctx context.Context, items memstash.List[K, V], ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(items))
	modelIndex := make(map[string]int, len(items))
	for _, item := range items {
		update, err := c.updateDoc(item.Value, ttl)
		if err != nil {
			return err
		}
		storageKey := c.keyFunc(item.Key)
		model := mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": storageKey}).
			SetUpdate(update).
			SetUpsert(true)
		if i, seen := modelIndex[storageKey]; seen {
			models[i] = model
			continue
		}
		modelIndex[storageKey] = len(models)
		models = append(models, model)
	}
	_, err := c.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// updateDoc builds the $set/$unset update for a value and TTL. With no TTL it clears any previous expiry so the item
// becomes permanent.
func (c *Cache[K, V]) updateDoc(value V, ttl time.Duration) (bson.M, error) {
	data, err := c.codec.Marshal(value)
	if err != nil {
		return nil, err
	}
	if ttl > 0 {
		return bson.M{"$set": bson.M{"v": data, "expireAt": time.Now().Add(ttl)}}, nil
	}
	return bson.M{"$set": bson.M{"v": data}, "$unset": bson.M{"expireAt": ""}}, nil
}

func isExpired(d doc) bool {
	return !d.ExpireAt.IsZero() && d.ExpireAt.Before(time.Now())
}
