package integration

import (
	"context"
	"errors"
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
		c, err := dynamo_adapter.NewCache[string, string](client, l2.StringCodec(), "memstash_cache", opts...)
		require.NoError(t, err, "NewCache")
		t.Cleanup(c.Close)
		return c
	})
}
