module github.com/zakonnic/memstash/l2/rainycape_adapter

go 1.24

require github.com/zakonnic/memstash v0.9.0

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/rainycape/memcache v0.0.0-20150622160815-1031fa0ce2f2
)

replace github.com/zakonnic/memstash => ../..
