module github.com/peterbourgon/tinys3

go 1.24.2

require (
	github.com/peterbourgon/ff/v4 v4.0.0-beta.1
	github.com/peterbourgon/unixtransport v0.0.6
)

replace github.com/peterbourgon/unixtransport => ../unixtransport
