module github.com/zakonnic/memstash/l2/valyala_adapter

go 1.24

require github.com/zakonnic/memstash v0.9.0

require (
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/valyala/ybc v0.0.0-20181207142156-3d480539f84d
)

replace github.com/zakonnic/memstash => ../..
