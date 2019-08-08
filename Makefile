.PHONY: test image unit_test

IMAGE ?= deitch/godiskfs:build

GOENV ?= GO111MODULE=on CGO_ENABLED=0

image:
	docker build -t $(IMAGE) testhelper/docker

# because we keep making the same typo
unit-test: unit_test
unit_test:
	@$(GOENV) go test ./...

test: image
	TEST_IMAGE=$(IMAGE) $(GOENV) go test ./...
