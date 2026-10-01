FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/servico-logistica .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/servico-logistica /servico-logistica
ENTRYPOINT ["/servico-logistica"]
