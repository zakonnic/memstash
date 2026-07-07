module github.com/zakonnic/memstash/l2/dynamo_adapter

go 1.24

require (
	github.com/aws/aws-sdk-go-v2 v1.42.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.60.0
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.30 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.7 // indirect
	github.com/aws/smithy-go v1.27.3 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
)

replace github.com/zakonnic/memstash => ../..
