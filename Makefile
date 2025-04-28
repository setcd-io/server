TEMPDIR ?= /tmp
GITHUB_ENV ?= $(TEMPDIR)/.env
CERTDIR ?= $(shell pwd)/server/certs
CERTFILE ?= localhost.crt
KEYFILE ?= localhost.key
KUBE_VERSION ?= 1.29.0
ETCD_HOST ?= host.docker.internal
CACHEDIR ?= .cache
SERVE ?= dynamodb-local
RUN ?= microsoft-etcd3
WHAT ?= 

logs:
	@docker compose \
		--project-directory $(PWD) \
		--project-name tests \
		logs \
			--timestamps \
			--no-log-prefix \
			| sort

test: _checkout-$(RUN) _patch-$(RUN) _test-$(RUN)
	@echo "Running tests for $(RUN) on $(SERVE)"
	docker compose \
		--project-directory $(PWD) \
		--project-name tests \
		down || true
	docker compose \
		--project-directory $(PWD) \
		--project-name tests \
		-f tests/docker-compose.$(SERVE).serve.yml \
		-f tests/docker-compose.$(RUN).test.yml \
		up \
			--build \
			--always-recreate-deps \
			--force-recreate \
			--remove-orphans \
			--exit-code-from test

_checkout:
	@mkdir -p $(CACHEDIR)

patches:
	@mkdir -p $(PWD)/tests/patches
	git -C $(CACHEDIR)/$(RUN) diff > $(PWD)/tests/patches/$(SERVE)+$(RUN).diff

_patch:
	@cp "$(PWD)/tests/patches/$(SERVE)+$(RUN).diff" "$(CACHEDIR)/$(RUN).diff"
	@cd $(CACHEDIR)/$(RUN) && \
		git reset --hard && \
		patch -p1 < ../$(RUN).diff

_test: _test-$(SERVE)
	echo "TESTS=\"$(WHAT)\"" > $(CACHEDIR)/$(RUN).env

_test-coreos-etcd:
	@echo "noop"

_test-dynamodb-remote:
	$(eval BRANCH := $(shell git rev-parse --abbrev-ref HEAD))
	echo "AWS_REGION=$(AWS_REGION)" > $(CACHEDIR)/$(SERVE).env
	echo "AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID)" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY)" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN)" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_DYNAMODB_TABLE_ETCD__NAME=etcd-test-branch-$(USER)-$(BRANCH)" >> $(CACHEDIR)/$(SERVE).env

_test-dynamodb-local:
	echo "AWS_REGION=us-east-1" > $(CACHEDIR)/$(SERVE).env
	echo "AWS_ACCESS_KEY_ID=DUMMYIDEXAMPLE" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_SECRET_ACCESS_KEY=DUMMYEXAMPLEKEY" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_ENDPOINT_URL_DYNAMODB=http://ddb:8000" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_ENDPOINT_URL_DYNAMODB_STREAMS=http://ddb:8000" >> $(CACHEDIR)/$(SERVE).env
	echo "AWS_DYNAMODB_TABLE_ETCD__NAME=etcd" >> $(CACHEDIR)/$(SERVE).env

### Microsoft etcd3 Tests ###
_checkout-microsoft-etcd3: _checkout
	@git clone https://github.com/microsoft/etcd3.git -b v1.1.2 $(CACHEDIR)/$(RUN) || true

_patch-microsoft-etcd3: _patch
	$(eval WHAT := $(if $(strip $(WHAT)), $(WHAT), crud|subscription|lock|transaction|unsubscribe|lease))

_test-microsoft-etcd3: _test

### Kubernetes etcd3 Tests ###
_checkout-kubernetes-etcd3: _checkout
	@git clone https://github.com/kubernetes/kubernetes.git -b v$(KUBE_VERSION) $(CACHEDIR)/$(RUN) || true

_patch-kubernetes-etcd3: _patch
	$(eval WHAT := $(if $(strip $(WHAT)), $(WHAT), ".*"))

_test-kubernetes-etcd3: _test
