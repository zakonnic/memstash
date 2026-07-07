module github.com/zakonnic/memstash/l2/tarantool_adapter

go 1.24

require (
	github.com/tarantool/go-tarantool/v2 v2.4.2
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/tarantool/go-iproto v1.1.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace github.com/zakonnic/memstash => ../..
