package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	mongo_adapter "github.com/zakonnic/memstash/l2/mongo_adapter"
)

func TestMongoAdapter(t *testing.T) {
	requireServer(t, mongoAddr())
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://memstash:memstash@"+mongoAddr()))
	require.NoError(t, err, "mongo.Connect")
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	coll := client.Database("memstash").Collection("cache")

	// The TTL index lets the server purge expired documents; the adapter hides them on read regardless.
	setup, err := mongo_adapter.New[string, string](coll, l2.StringCodec())
	require.NoError(t, err, "New")
	require.NoError(t, setup.EnsureTTLIndex(ctx), "EnsureTTLIndex")

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := mongo_adapter.NewCache[string, string](coll, l2.StringCodec(), opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
