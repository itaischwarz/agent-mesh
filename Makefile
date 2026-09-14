.PHONY: proto generate manifests build run deploy undeploy fmt vet

# Regenerates pkg/a2a/pb from pkg/a2a/proto/a2a.proto.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
proto:
	mkdir -p pkg/a2a/pb
	protoc \
		--go_out=pkg/a2a/pb --go_opt=paths=source_relative \
		--go-grpc_out=pkg/a2a/pb --go-grpc_opt=paths=source_relative \
		-I pkg/a2a/proto pkg/a2a/proto/a2a.proto


proto-mesh:
	mkdir -p pkg/mesh/pb
	protoc \
		--go_out=pkg/mesh/pb --go_opt=paths=source_relative \
		--go-grpc_out=pkg/mesh/pb --go-grpc_opt=paths=source_relative \
		-I pkg/mesh/proto -I pkg/a2a/proto pkg/mesh/proto/mesh.proto

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
	kubectl apply -f config/rbac/role.yaml
	kubectl apply -f config/manager/manager.yaml

undeploy:
	kubectl delete -f config/manager/manager.yaml --ignore-not-found
	kubectl delete -f config/rbac/role.yaml --ignore-not-found
	kubectl delete -f config/crd/bases/ --ignore-not-found

fmt:
	go fmt ./...

vet:
	go vet ./...
