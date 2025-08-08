test: ## Run all unit tests.
	go test -timeout 15m ./...

test-with-race: ## Run all unit tests with data race detection.
	go test -timeout 15m -race -count 1 ./...

test-coverage: ## run test coverage with coverage output.
	go test -timeout 15m -race -count 1 -coverprofile=coverage.txt ./...
	@go tool cover -html=coverage.txt
	@go tool cover -func=coverage.txt | tail -n1

go-version:
	@go version
