module github.com/zakonnic/memstash/l2/pgx_adapter

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/zakonnic/memstash v0.9.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/zakonnic/memstash => ../..
