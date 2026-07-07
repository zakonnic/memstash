module github.com/zakonnic/memstash/l2/redigo_adapter

go 1.24

require github.com/zakonnic/memstash v0.9.0

require (
	github.com/gomodule/redigo v1.9.3
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
)

replace github.com/zakonnic/memstash => ../..
