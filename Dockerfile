# syntax=docker/dockerfile:1

# Build a static binary. The SQLite driver is pure Go, so CGO stays off and
# the result runs on a distroless base with no libc.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/vigil ./cmd/vigil \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/vigil /vigil
COPY --from=build --chown=nonroot:nonroot /out/data /data
ENV VIGIL_DB=/data/vigil.db \
    VIGIL_CONFIG=/etc/vigil/vigil.yaml \
    VIGIL_LOG_FORMAT=json
VOLUME /data
EXPOSE 4210
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=4s --start-period=5s CMD ["/vigil", "healthcheck"]
ENTRYPOINT ["/vigil"]
CMD ["server"]
