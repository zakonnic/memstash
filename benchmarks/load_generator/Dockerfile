# The load generator as an image, run next to the L2 servers by docker/docker-compose.yml (make
# load-generator-docker). The build context is the repo root: go.work resolves the core and the adapters from it.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /load-generator ./benchmarks/load_generator/cmd/load-generator

FROM scratch
COPY --from=build /load-generator /load-generator
ENTRYPOINT ["/load-generator"]
