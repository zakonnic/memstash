module github.com/zakonnic/memstash/benchmarks/load_generator

go 1.25.0

require (
	github.com/bradfitz/gomemcache v0.0.0-20260422231931-4d751bb6e37c
	github.com/gomodule/redigo v1.9.3
	github.com/joomcode/redispipe v0.9.8
	github.com/memcachier/mc/v3 v3.0.3
	github.com/rainycape/memcache v0.0.0-20150622160815-1031fa0ce2f2
	github.com/redis/go-redis/v9 v9.22.0
	github.com/redis/rueidis v1.0.76
	github.com/stretchr/testify v1.11.1
	github.com/valkey-io/valkey-go v1.0.76
	github.com/zakonnic/memstash v0.9.6
	github.com/zakonnic/memstash/l2/gomemcache_adapter v0.9.6
	github.com/zakonnic/memstash/l2/goredis_adapter v0.9.6
	github.com/zakonnic/memstash/l2/mc_adapter v0.9.6
	github.com/zakonnic/memstash/l2/rainycape_adapter v0.9.6
	github.com/zakonnic/memstash/l2/redigo_adapter v0.9.6
	github.com/zakonnic/memstash/l2/redispipe_adapter v0.9.6
	github.com/zakonnic/memstash/l2/rueidis_adapter v0.9.6
	github.com/zakonnic/memstash/l2/valkey_adapter v0.0.0-00010101000000-000000000000
	golang.org/x/sys v0.47.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/joomcode/errorx v1.2.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/zakonnic/memstash => ../..

replace github.com/zakonnic/memstash/l2/rueidis_adapter => ../../l2/rueidis_adapter

replace github.com/zakonnic/memstash/l2/gomemcache_adapter => ../../l2/gomemcache_adapter

replace github.com/zakonnic/memstash/l2/goredis_adapter => ../../l2/goredis_adapter

replace github.com/zakonnic/memstash/l2/mc_adapter => ../../l2/mc_adapter

replace github.com/zakonnic/memstash/l2/rainycape_adapter => ../../l2/rainycape_adapter

replace github.com/zakonnic/memstash/l2/redigo_adapter => ../../l2/redigo_adapter

replace github.com/zakonnic/memstash/l2/redispipe_adapter => ../../l2/redispipe_adapter

replace github.com/zakonnic/memstash/l2/valkey_adapter => ../../l2/valkey_adapter
