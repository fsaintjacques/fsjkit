GOLANGCI_LINT = $(shell go env GOPATH)/bin/golangci-lint

.PHONY: test
test:
	go list -f '{{.Dir}}/...' -m | xargs go test -v

.PHONY: lint
lint:
	go list -f '{{.Dir}}/...' -m | xargs $(GOLANGCI_LINT) run --config=.golangci.yaml