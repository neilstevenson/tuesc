FROM registry.access.redhat.com/ubi9/go-toolset:latest

WORKDIR /build

RUN echo Version 3

COPY go.mod /build/
COPY go.sum /build/
COPY cli/   /build/cli/

ENV MY_COUNT ""
ENV MY_HOST ""
ENV MY_INPUT_FILE ""
ENV MY_MAP_NAME ""
ENV MY_NEAR_CACHE ""

RUN go mod download
RUN go build -o /tmp/client ./cli/client

CMD ["/tmp/client"]
