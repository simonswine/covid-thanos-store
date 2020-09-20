FROM alpine:3.12

LABEL maintainer="Christian Simon <simon@swine.de" \
      org.opencontainers.image.title="coivd-thanos-store" \
      org.opencontainers.image.description="Use ECDC Covid statistics as Thanos metrics store" \
      org.opencontainers.image.url="https://github.com/simonswine/covid-thanos-store" \
      org.opencontainers.image.source="git@github.com:simonswine/covid-thanos-store"

RUN mkdir -p /cache && chown nobody:nogroup /cache
VOLUME /cache
WORKDIR /cache

COPY covid-thanos-store /usr/local/bin/
USER nobody

ENTRYPOINT ["/usr/local/bin/covid-thanos-store"]
