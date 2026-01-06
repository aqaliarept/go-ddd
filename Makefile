.PHONY: test lint build test-mongo test-coverage coverage-html

build:
	go build work

test:
	go test work -race

test-coverage:
	@tools/test-coverage.sh

lint:
	tools/lint.sh

ci: lint build test-coverage test