module memstash-bench

go 1.24.0

require (
	github.com/Yiling-J/theine-go v0.6.2
	github.com/allegro/bigcache/v3 v3.1.0
	github.com/coocood/freecache v1.2.7
	github.com/dgraph-io/ristretto/v2 v2.4.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/maypok86/otter/v2 v2.3.0
	github.com/puzpuzpuz/xsync/v3 v3.5.1
	github.com/zakonnic/memstash v0.9.0
	golang.org/x/sys v0.36.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
)

replace github.com/zakonnic/memstash => ../
