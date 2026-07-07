module github.com/zakonnic/memstash/l2/aerospike_adapter

go 1.24

require (
	github.com/aerospike/aerospike-client-go/v7 v7.10.2
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240711142825-46eb208f015d // indirect
	google.golang.org/grpc v1.63.3 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/zakonnic/memstash => ../..
