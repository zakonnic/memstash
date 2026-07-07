module github.com/zakonnic/memstash/l2/redispipe_adapter

go 1.24

require github.com/zakonnic/memstash v0.9.0

require (
	github.com/joomcode/errorx v1.0.3 // indirect
	github.com/joomcode/redispipe v0.9.7
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
)

replace github.com/zakonnic/memstash => ../..
