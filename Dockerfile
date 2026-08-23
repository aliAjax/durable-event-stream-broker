FROM golang:1.23 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/broker ./cmd/broker
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/broker /broker
EXPOSE 8080
ENTRYPOINT ["/broker"]
