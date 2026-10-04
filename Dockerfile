FROM golang:1.25 AS builder
WORKDIR /workspace
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -o /manager .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
