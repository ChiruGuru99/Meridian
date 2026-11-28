FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 go build -trimpath -o /out/railsim ./cmd/railsim

FROM alpine:3.24
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
EXPOSE 8080 8082
ENTRYPOINT ["gateway"]
