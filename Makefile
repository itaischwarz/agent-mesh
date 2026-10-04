.PHONY: generate manifests build run deploy deploy-local undeploy fmt vet test \
	docker-build-manager docker-build-echoagent docker-build kind-load test-a2a

IMG_TAG ?= dev
MANAGER_IMG ?= agent-mesh:$(IMG_TAG)
ECHOAGENT_IMG ?= echoagent:$(IMG_TAG)
KIND_CLUSTER ?= agent-mesh

# A2A wire types come from the official SDK (github.com/a2aproject/a2a-go),
# so there is no protoc step.

# Regenerates zz_generated.deepcopy.go from the +kubebuilder:object markers.
# Requires: controller-gen (go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest)
generate:
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."

# Regenerates config/crd/bases/*.yaml and config/rbac/role.yaml from markers.
manifests:
	controller-gen rbac:roleName=agent-mesh-manager-role crd webhook \
		paths="./..." output:crd:artifacts:config=config/crd/bases

build:
	go build -o bin/manager main.go

run: build
	./bin/manager

deploy:
	kubectl apply -f config/crd/bases/
	kubectl apply -f config/rbac/role.yaml -f config/rbac/role_binding.yaml
	kubectl apply -f config/manager/manager.yaml

undeploy:
	kubectl delete -f config/manager/manager.yaml --ignore-not-found
	kubectl delete -f config/rbac/role_binding.yaml -f config/rbac/role.yaml --ignore-not-found
	kubectl delete -f config/crd/bases/ --ignore-not-found

# Same as deploy, but against a local kind cluster: builds+loads the images
# (kind-load) and swaps manager.yaml's ghcr.io reference for the locally
# loaded $(MANAGER_IMG) on the fly, so it never tries to pull from a registry.
deploy-local: kind-load
	kubectl apply -f config/crd/bases/
	kubectl apply -f config/rbac/role.yaml
	sed 's#ghcr.io/itaic/agent-mesh:latest#$(MANAGER_IMG)#' config/manager/manager.yaml | kubectl apply -f -

fmt:
	go fmt ./...

vet:
	go vet ./...

# Builds the manager image from the root Dockerfile.
docker-build-manager:
	docker build -t $(MANAGER_IMG) -f Dockerfile .

# Builds the reference echoagent image (cmd/echoagent) — a minimal
# A2AServiceServer for local testing, not for production use.
docker-build-echoagent:
	docker build -t $(ECHOAGENT_IMG) -f cmd/echoagent/Dockerfile .

docker-build: docker-build-manager docker-build-echoagent

# Loads both images into a running kind cluster (override KIND_CLUSTER if
# yours isn't named "agent-mesh") so config/manager/manager.yaml and an
# Agent CR can reference them without pulling from a registry.
kind-load: docker-build
	kind load docker-image $(MANAGER_IMG) $(ECHOAGENT_IMG) --name $(KIND_CLUSTER)

# Smoke-tests the general A2A protocol round-trip against the containerized
# echoagent image (docker build + docker run, no kind/cluster required).
# Requires docker and grpcurl on PATH (brew install grpcurl).
test-a2a:
	./hack/test-a2a.sh
