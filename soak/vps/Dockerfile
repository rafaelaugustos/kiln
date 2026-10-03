FROM golang:1.27 AS build
WORKDIR /src
COPY . .
RUN cd soak && GOWORK=off CGO_ENABLED=0 go build -o /soak .

FROM docker:27-cli
COPY --from=build /soak /usr/local/bin/soak
COPY soak/vps/run.sh /usr/local/bin/run.sh
ENTRYPOINT ["/bin/sh", "/usr/local/bin/run.sh"]
