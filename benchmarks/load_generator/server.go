package load_generator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	gomemcachelib "github.com/bradfitz/gomemcache/memcache"
	redigolib "github.com/gomodule/redigo/redis"
	redispiperedis "github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
	mclib "github.com/memcachier/mc/v3"
	rainycapelib "github.com/rainycape/memcache"
	goredislib "github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
	valkeylib "github.com/valkey-io/valkey-go"
	"github.com/zakonnic/memstash"
	gomemcache_adapter "github.com/zakonnic/memstash/l2/gomemcache_adapter"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
	mc_adapter "github.com/zakonnic/memstash/l2/mc_adapter"
	rainycape_adapter "github.com/zakonnic/memstash/l2/rainycape_adapter"
	redigo_adapter "github.com/zakonnic/memstash/l2/redigo_adapter"
	redispipe_adapter "github.com/zakonnic/memstash/l2/redispipe_adapter"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
	valkey_adapter "github.com/zakonnic/memstash/l2/valkey_adapter"
)

// ServerType picks the L2 adapter behind Scenario.Address, named after its l2/ directory. Only the adapters whose
// client is built from an address alone are here - the rest need a DSN, credentials or a schema, and a scenario has
// nowhere to put those.
type ServerType string

const (
	// Rueidis is the default: github.com/redis/rueidis, one address or a cluster.
	Rueidis ServerType = "rueidis"
	// Valkey is github.com/valkey-io/valkey-go, one address or a cluster.
	Valkey ServerType = "valkey"
	// GoRedis is github.com/redis/go-redis, one address or a cluster.
	GoRedis ServerType = "goredis"
	// Redigo is github.com/gomodule/redigo. Single node only - the client has no cluster mode.
	Redigo ServerType = "redigo"
	// Redispipe is github.com/joomcode/redispipe, one address or a cluster.
	Redispipe ServerType = "redispipe"
	// Gomemcache is github.com/bradfitz/gomemcache. Several addresses are the client's own server ring, not a cluster.
	Gomemcache ServerType = "gomemcache"
	// Rainycape is github.com/rainycape/memcache, addressed like Gomemcache.
	Rainycape ServerType = "rainycape"
	// Mc is github.com/memcachier/mc, addressed like Gomemcache. Dialed without SASL credentials.
	Mc ServerType = "mc"
)

// ServerTypes lists every value a Scenario accepts, memcached backends last.
func ServerTypes() []ServerType {
	return []ServerType{Rueidis, Valkey, GoRedis, Redigo, Redispipe, Gomemcache, Rainycape, Mc}
}

// singleNodeOnly are the types that cannot take more than one address.
var singleNodeOnly = []ServerType{Redigo}

func (s ServerType) valid() bool { return slices.Contains(ServerTypes(), s) }

// openL2 dials the scenario's backend and builds the cache over it. The returned func closes the client; the cache's
// own Close does not, the client's lifecycle stays with whoever dialed it.
func openL2[K comparable, V any](server ServerType, addrs []string, codec memstash.Codec[V],
	opts []memstash.Option) (*memstash.Cache[K, V], func(), error) {

	switch server {
	case Rueidis:
		client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: addrs})
		if err != nil {
			return nil, nil, err
		}
		cache, err := rueidis_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, client.Close, err)

	case Valkey:
		client, err := valkeylib.NewClient(valkeylib.ClientOption{InitAddress: addrs})
		if err != nil {
			return nil, nil, err
		}
		cache, err := valkey_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, client.Close, err)

	case GoRedis:
		var client interface {
			goredislib.Cmdable
			Close() error
		}
		if len(addrs) == 1 {
			client = goredislib.NewClient(&goredislib.Options{Addr: addrs[0]})
		} else {
			client = goredislib.NewClusterClient(&goredislib.ClusterOptions{Addrs: addrs})
		}
		cache, err := goredis_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, func() { client.Close() }, err)

	case Redigo:
		// DialContext (rather than Dial) makes the pool's connections context-aware, so the adapter can cancel
		// commands, not just the wait for a free connection.
		pool := &redigolib.Pool{
			DialContext: func(ctx context.Context) (redigolib.Conn, error) {
				return redigolib.DialContext(ctx, "tcp", addrs[0])
			},
			MaxIdle: 32,
		}
		cache, err := redigo_adapter.NewCache[K, V](pool, codec, opts...)
		return withClient(cache, func() { pool.Close() }, err)

	case Redispipe:
		var sender redispiperedis.Sender
		var err error
		if len(addrs) == 1 {
			sender, err = redisconn.Connect(context.Background(), addrs[0], redisconn.Opts{})
		} else {
			sender, err = rediscluster.NewCluster(context.Background(), addrs, rediscluster.Opts{})
		}
		if err != nil {
			return nil, nil, err
		}
		cache, err := redispipe_adapter.NewCache[K, V](sender, codec, opts...)
		return withClient(cache, sender.Close, err)

	case Gomemcache:
		client := gomemcachelib.New(addrs...)
		client.MaxIdleConns = 32 // the default is 2: too few for concurrent batch (GetMulti) traffic
		cache, err := gomemcache_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, func() { client.Close() }, err)

	case Rainycape:
		client, err := rainycapelib.New(addrs...)
		if err != nil {
			return nil, nil, err
		}
		client.SetMaxIdleConnsPerAddr(32) // more room for concurrent batch (GetMulti) traffic
		cache, err := rainycape_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, func() { client.Close() }, err)

	case Mc:
		// Plain binary protocol, no SASL: the empty credentials are what a stock memcached expects.
		client := mclib.NewMC(strings.Join(addrs, ","), "", "")
		cache, err := mc_adapter.NewCache[K, V](client, codec, opts...)
		return withClient(cache, client.Quit, err)
	}
	return nil, nil, fmt.Errorf("unknown l2 server type %q, want one of %v", server, ServerTypes())
}

// withClient hands back the dialed client's closer, or releases it when the adapter refused it - a scenario that
// fails to build must not leave a connection behind.
func withClient[K comparable, V any](cache *memstash.Cache[K, V], closeClient func(),
	err error) (*memstash.Cache[K, V], func(), error) {

	if err != nil {
		closeClient()
		return nil, nil, err
	}
	return cache, closeClient, nil
}
