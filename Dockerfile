FROM alpine:3.12

RUN mkdir -p /cache && chown nobody:nogroup /cache
VOLUME /cache
WORKDIR /cache

COPY covid-thanos-store /usr/local/bin/
USER nobody

ENTRYPOINT ["/usr/local/bin/covid-thanos-store"]
