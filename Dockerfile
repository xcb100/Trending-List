ARG GO_VERSION=1.25.6

FROM golang:${GO_VERSION}-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY main.go ./

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0
ENV GOTELEMETRY=off

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/leaderboard-service .

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/leaderboard-service /app/leaderboard-service

EXPOSE 8080
EXPOSE 9090

ENV BUSINESS_ADDR=:8080
ENV INTERNAL_ADDR=:9090
ENV GOTELEMETRY=off

ENTRYPOINT ["/app/leaderboard-service"]
