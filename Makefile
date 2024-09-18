ENV=$$(cat .env)
EXE=$$(uname -a | grep -v NT & echo .exe)

test-race:
	env $(ENV) go test -v -race

test:
	env $(ENV) go test -v

build:
	go build -o bin/db_explorer$(EXE) $$(ls *.go | grep -v _test.go)

run: build
	env $(ENV) bin/db_explorer$(EXE)

.PHONY: test-race test build run
