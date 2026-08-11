module github.com/zakonnic/memstash/l2/aerospike_adapter

go 1.25.0

require (
	github.com/aerospike/aerospike-client-go/v7 v7.10.2
	github.com/zakonnic/memstash v0.9.6
)

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/grpc v1.83.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/zakonnic/memstash => ../..
