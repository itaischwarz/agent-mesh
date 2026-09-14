// Package mesh holds constants shared by both sides of MeshService: the
// in-cluster server (internal/meshserver, wired up in main.go) and its
// client SDK (pkg/mesh/client), so a default only needs to live in one place.
package mesh

// DefaultAddr is the mesh gRPC service's default bind address (server side)
// and default dial target (client side) when no override is given.
const DefaultAddr = ":50052"
