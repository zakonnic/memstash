module github.com/zakonnic/memstash/l2/rueidis_adapter

go 1.25.0

require (
	github.com/redis/rueidis v1.0.76
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/zakonnic/memstash => ../..
