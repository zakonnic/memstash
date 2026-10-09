package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	dynamo_adapter "github.com/zakonnic/memstash/l2/dynamo_adapter"
)

func TestDynamoAdapter(t *testing.T) {
	requireServer(t, dynamoAddr())
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	require.NoError(t, err, "aws config")
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + dynamoAddr())
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("memstash_cache"),
		AttributeDefinitions: []dynamotypes.AttributeDefinition{
			{AttributeName: aws.String("cache_key"), AttributeType: dynamotypes.ScalarAttributeTypeS},
		},
		KeySchema: []dynamotypes.KeySchemaElement{
			{AttributeName: aws.String("cache_key"), KeyType: dynamotypes.KeyTypeHash},
		},
		BillingMode: dynamotypes.BillingModePayPerRequest,
	})
	var exists *dynamotypes.ResourceInUseException
	if err != nil && !errors.As(err, &exists) {
		require.NoError(t, err, "create table")
	}

	runSuite(t, func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
		opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
		c, err := dynamo_adapter.NewCache[string, string](client, suiteCodec, "memstash_cache", opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})

	// Keys the service rejects as they are and a nil value must work like any other, alone and in a batch.
	t.Run("AwkwardKeysAndNilValue", func(t *testing.T) {
		store, err := dynamo_adapter.New[string, []byte](client, l2.BytesCodec(), "memstash_cache")
		require.NoError(t, err)
		keys := []string{uniquePrefix(t), "", strings.Repeat("k", 3000)}
		for _, key := range keys {
			require.NoError(t, store.Set(ctx, key, nil, 0), "Set %.20q", key)
			_, ok, err := store.Get(ctx, key)
			require.NoError(t, err)
			require.True(t, ok, "Get %.20q", key)
		}
		items := memstash.List[string, []byte]{}
		for _, key := range keys {
			items = append(items, memstash.KeyVal[string, []byte]{Key: key})
		}
		require.NoError(t, store.BatchSet(ctx, items, 0))
		found, err := store.BatchGet(ctx, keys)
		require.NoError(t, err)
		require.Len(t, found, len(keys))
		require.NoError(t, store.BatchDelete(ctx, keys))
	})
}
