FROM golang:1.23 AS builder
WORKDIR /workspace
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /echoagent ./cmd/echoagent

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /echoagent /echoagent
USER 65532:65532
EXPOSE 50051
ENTRYPOINT ["/echoagent"]
