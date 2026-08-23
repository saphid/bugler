FROM oven/bun:1.4.0 AS build
WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
RUN bun install --frozen-lockfile
COPY src ./src
RUN bun build --compile --target=bun-linux-x64 src/bugler.ts --outfile /bugler

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --home /data --shell /usr/sbin/nologin bugler \
  && install -d -o bugler -g bugler -m 0750 /data
COPY --from=build /bugler /usr/local/bin/bugler
COPY deploy/docker/entrypoint.sh /usr/local/bin/bugler-entrypoint
RUN chmod 0755 /usr/local/bin/bugler /usr/local/bin/bugler-entrypoint
USER bugler
VOLUME ["/data"]
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/bugler-entrypoint"]
