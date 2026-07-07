module github.com/zakonnic/memstash/l2/goredis_adapter

go 1.24

require (
	github.com/redis/go-redis/v9 v9.21.0
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/zakonnic/memstash => ../..
