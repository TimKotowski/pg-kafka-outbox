test:
	@go install gotest.tools/gotestsum@latest

	@mkdir -p tmp/
	@gotestsum \
		--junitfile tmp/test-report.xml \
		--format pkgname-and-test-fails \
		-- \
		-race \
		-coverprofile=tmp/coverage.txt \
		-failfast \
		-shuffle=on \
		-p=2 \
		-covermode=atomic \
		./...

coverage:
	@go tool cover -html=tmp/coverage.txt

go-version:
	@go version

.PHONY: dev
dev:
	docker compose -f ./compose/compose.yaml up

.PHONY: clean
clean:
	docker compose -f ./compose/compose.yaml down
