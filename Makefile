.PHONY: test image unit_test

IMAGE ?= deitch/godiskfs:build

image:
	docker build -t $(IMAGE) testhelper/docker

unit_test:
	@go test ./...

test: image
	TEST_IMAGE=$(IMAGE) go test ./...
