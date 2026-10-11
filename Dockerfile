FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=1.1.1
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/hotkv-ltd/hotkv-gateway/internal/gateway.Version=${VERSION}" -o /out/hotkv-gateway ./cmd/hotkv-gateway

# distroless/static has CA certificates (for HTTPS to the LLM API) and nothing else
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hotkv-gateway /hotkv-gateway
EXPOSE 8080
ENV HOTKV_GATEWAY_LISTEN=0.0.0.0:8080
ENTRYPOINT ["/hotkv-gateway"]
