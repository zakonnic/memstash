module github.com/zakonnic/memstash/benchmarks/load_generator

go 1.25.0

require (
	github.com/dustin/go-humanize v1.0.1
	github.com/puzpuzpuz/xsync/v3 v3.5.1
	github.com/redis/rueidis v1.0.76
	github.com/stretchr/testify v1.11.1
	github.com/zakonnic/memstash v0.9.6
	github.com/zakonnic/memstash/l2/rueidis_adapter v0.9.6
	golang.org/x/sys v0.47.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
)

replace github.com/zakonnic/memstash => ../..

replace github.com/zakonnic/memstash/l2/rueidis_adapter => ../../l2/rueidis_adapter
