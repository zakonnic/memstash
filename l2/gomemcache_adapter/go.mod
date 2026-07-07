module github.com/zakonnic/memstash/l2/gomemcache_adapter

go 1.24

require github.com/zakonnic/memstash v0.9.0

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/bradfitz/gomemcache v0.0.0-20260422231931-4d751bb6e37c
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
)

replace github.com/zakonnic/memstash => ../..
