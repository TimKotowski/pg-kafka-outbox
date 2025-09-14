.PHONY:
test:
	@go install gotest.tools/gotestsum@latest

	@mkdir -p tmp/
	@gotestsum \
		--junitfile tmp/test-report.xml \
		-- \
		-race \
		-count 1 \
		-coverprofile=tmp/coverage.out \
		./...

coverage:
	@go tool cover -html=tmp/coverage.out

.PHONY:
go-version:
	@go version

.PHONY:
dev:
	docker compose -f ./compose/compose.yaml up

.PHONY: clean
clean:
	docker compose -f ./compose/compose.yaml down
