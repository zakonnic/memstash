package dynamo_adapter

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
)

// throttledDynamo stores items in a map. It hands back every write unprocessed for its first stalls calls, and
// answers a BatchGetItem with at most page items, the rest unprocessed, as the service does past 16 MB.
type throttledDynamo struct {
	DynamoAPI
	items  map[string]map[string]types.AttributeValue
	stalls int
	page   int
}

func (d *throttledDynamo) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if d.stalls > 0 {
		d.stalls--
		return &dynamodb.BatchWriteItemOutput{UnprocessedItems: in.RequestItems}, nil
	}
	for _, writes := range in.RequestItems {
		for _, w := range writes {
			if w.PutRequest != nil {
				d.items[w.PutRequest.Item[keyAttr].(*types.AttributeValueMemberS).Value] = w.PutRequest.Item
			}
		}
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (d *throttledDynamo) BatchGetItem(_ context.Context, in *dynamodb.BatchGetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	out := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]types.AttributeValue{}}
	for table, req := range in.RequestItems {
		keys := req.Keys
		if len(keys) > d.page {
			out.UnprocessedKeys = map[string]types.KeysAndAttributes{table: {Keys: keys[d.page:]}}
			keys = keys[:d.page]
		}
		for _, key := range keys {
			if item, ok := d.items[key[keyAttr].(*types.AttributeValueMemberS).Value]; ok {
				out.Responses[table] = append(out.Responses[table], item)
			}
		}
	}
	return out, nil
}

func TestBatchesOutlastThrottlingAndPaging(t *testing.T) {
	ctx := context.Background()
	store := &throttledDynamo{items: map[string]map[string]types.AttributeValue{}, stalls: 3, page: 10}
	c, err := New[string, string](store, l2.StringCodec(), "t")
	if err != nil {
		t.Fatal(err)
	}
	items := memstash.List[string, string]{}
	keys := []string{}
	for i := range 25 {
		items = append(items, memstash.KeyVal[string, string]{Key: strconv.Itoa(i), Value: "v"})
		keys = append(keys, strconv.Itoa(i))
	}
	if err := c.BatchSet(ctx, items, 0); err != nil {
		t.Fatalf("BatchSet gave up on three throttled rounds: %v", err)
	}
	found, err := c.BatchGet(ctx, keys)
	if err != nil || len(found) != len(keys) {
		t.Fatalf("BatchGet paged 10 at a time: %d of %d, err %v", len(found), len(keys), err)
	}

	store.stalls = maxStalls + 1
	if err := c.BatchSet(ctx, items, 0); !errors.Is(err, errUnprocessed) {
		t.Fatalf("a batch that never goes through: %v", err)
	}
}
