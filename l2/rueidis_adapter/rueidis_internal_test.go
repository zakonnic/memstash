package rueidis_adapter

import (
	"testing"

	rueidislib "github.com/redis/rueidis"

	"github.com/zakonnic/memstash/l2"
)

// oneMasterCluster is a cluster client that knows a single node, as it does against a one-shard cluster.
type oneMasterCluster struct{ rueidislib.Client }

func (oneMasterCluster) Mode() rueidislib.ClientMode { return rueidislib.ClientModeCluster }

func (oneMasterCluster) Nodes() map[string]rueidislib.Client {
	return map[string]rueidislib.Client{"127.0.0.1:7000": nil}
}

// A cluster client rejects a cross-slot MGET or dedicated pipeline even when the cluster has one master, so it must
// never take the single-node paths.
func TestOneMasterClusterIsNotSingleNode(t *testing.T) {
	client := oneMasterCluster{}
	if IsOrderedMgetAvailable(client) {
		t.Error("ordered MGET detected on a cluster client")
	}
	c, err := New[string, string](client, l2.StringCodec())
	if err != nil {
		t.Fatal(err)
	}
	if c.singleNode || c.orderedMget {
		t.Errorf("a one-master cluster client took the single-node paths: singleNode=%v orderedMget=%v", c.singleNode, c.orderedMget)
	}
}
