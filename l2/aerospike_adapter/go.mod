module github.com/zakonnic/memstash/l2/aerospike_adapter

go 1.25.0

require (
	github.com/aerospike/aerospike-client-go/v7 v7.10.2
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/zakonnic/memstash => ../..
