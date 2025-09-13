.PHONY:
test:
	@mkdir -p tmp/
	@go install github.com/jstemmer/go-junit-report/v2@latest
	@go install gotest.tools/gotestsum@latest
	@gotestsum \
		--junitfile tmp/test-report.xml \
		--format testname \
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
