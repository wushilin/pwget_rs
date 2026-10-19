# Packages the static musl binaries produced by the release workflow (docker-bin/).
FROM alpine:3.22
ARG TARGETARCH
ARG TARGETVARIANT
RUN apk add --no-cache ca-certificates
COPY docker-bin/ /tmp/bin/
RUN set -eu; \
    case "${TARGETARCH}${TARGETVARIANT}" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      armv7) target=armv7-unknown-linux-musleabihf ;; \
      *) echo "unsupported platform ${TARGETARCH}${TARGETVARIANT}" >&2; exit 1 ;; \
    esac; \
    install -m 755 "/tmp/bin/pwget-${target}" /usr/local/bin/pwget; \
    rm -rf /tmp/bin
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/pwget"]
