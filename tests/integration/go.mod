module memstash-integration

go 1.25.0

require (
	github.com/aerospike/aerospike-client-go/v7 v7.10.2
	github.com/aws/aws-sdk-go-v2 v1.43.4
	github.com/aws/aws-sdk-go-v2/config v1.32.35
	github.com/aws/aws-sdk-go-v2/credentials v1.19.34
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.63.1
	github.com/bradfitz/gomemcache v0.0.0-20260422231931-4d751bb6e37c
	github.com/go-sql-driver/mysql v1.10.0
	github.com/gomodule/redigo v1.9.3
	github.com/jackc/pgx/v5 v5.10.0
	github.com/joomcode/redispipe v0.9.8
	github.com/memcachier/mc/v3 v3.0.3
	github.com/rainycape/memcache v0.0.0-20150622160815-1031fa0ce2f2
	github.com/redis/go-redis/v9 v9.22.0
	github.com/redis/rueidis v1.0.76
	github.com/stretchr/testify v1.11.1
	github.com/tarantool/go-tarantool/v2 v2.4.2
	github.com/valkey-io/valkey-go v1.0.76
	github.com/zakonnic/memstash v0.9.6
	github.com/zakonnic/memstash/l2/gomemcache_adapter v0.9.0
	github.com/zakonnic/memstash/l2/goredis_adapter v0.9.0
	github.com/zakonnic/memstash/l2/mc_adapter v0.9.0
	github.com/zakonnic/memstash/l2/rainycape_adapter v0.9.0
	github.com/zakonnic/memstash/l2/redigo_adapter v0.9.0
	github.com/zakonnic/memstash/l2/redispipe_adapter v0.9.0
	github.com/zakonnic/memstash/l2/rueidis_adapter v0.9.0
	github.com/zakonnic/memstash/l2/valkey_adapter v0.9.0
	go.mongodb.org/mongo-driver v1.17.9
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.12.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.33.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.38.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.4 // indirect
	github.com/aws/smithy-go v1.27.6 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/montanaflynn/stats v0.12.2 // indirect
	github.com/tarantool/go-iproto v1.1.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/grpc v1.83.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/joomcode/errorx v1.2.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/zakonnic/memstash/l2/aerospike_adapter v0.9.0
	github.com/zakonnic/memstash/l2/dynamo_adapter v0.9.0
	github.com/zakonnic/memstash/l2/mongo_adapter v0.9.0
	github.com/zakonnic/memstash/l2/pgx_adapter v0.9.0
	github.com/zakonnic/memstash/l2/sql_adapter v0.9.0
	github.com/zakonnic/memstash/l2/tarantool_adapter v0.9.0
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/zakonnic/memstash => ../..
	github.com/zakonnic/memstash/l2/gomemcache_adapter => ../../l2/gomemcache_adapter
	github.com/zakonnic/memstash/l2/goredis_adapter => ../../l2/goredis_adapter
	github.com/zakonnic/memstash/l2/mc_adapter => ../../l2/mc_adapter
	github.com/zakonnic/memstash/l2/rainycape_adapter => ../../l2/rainycape_adapter
	github.com/zakonnic/memstash/l2/redigo_adapter => ../../l2/redigo_adapter
	github.com/zakonnic/memstash/l2/redispipe_adapter => ../../l2/redispipe_adapter
	github.com/zakonnic/memstash/l2/rueidis_adapter => ../../l2/rueidis_adapter
	github.com/zakonnic/memstash/l2/valkey_adapter => ../../l2/valkey_adapter
)

replace github.com/zakonnic/memstash/l2/pgx_adapter => ../../l2/pgx_adapter

replace github.com/zakonnic/memstash/l2/sql_adapter => ../../l2/sql_adapter

replace github.com/zakonnic/memstash/l2/mongo_adapter => ../../l2/mongo_adapter

replace github.com/zakonnic/memstash/l2/dynamo_adapter => ../../l2/dynamo_adapter

replace github.com/zakonnic/memstash/l2/aerospike_adapter => ../../l2/aerospike_adapter

replace github.com/zakonnic/memstash/l2/tarantool_adapter => ../../l2/tarantool_adapter
