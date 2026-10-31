# One image for every role: the head, the workers and the driver all run the
# same binary, and DISTA_ROLE picks which. Build it from the repository root:
#
#     docker build -f deploy/Dockerfile -t dista-smoke .
#
# PROGRAM is the Mojo file to build; its tasks are the cluster's registry.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS build
ARG PROGRAM=deploy/smoke.mojo
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /dista
COPY pixi.toml pixi.lock ./
RUN pixi install --locked -e default
COPY src src
COPY deploy deploy
COPY examples examples
RUN mkdir -p /out && pixi run -e default mojo build -I src ${PROGRAM} -o /out/dista
# The binary loads the Mojo runtime from the pixi environment; take along the
# libraries it links, and nothing else.
RUN mkdir -p /out/lib && ldd /out/dista | awk '/=> \/dista\/.pixi/ {print $3}' \
    | xargs -r cp -L -t /out/lib

FROM debian:bookworm-slim
COPY --from=build /out/lib /opt/dista/lib
COPY --from=build /out/dista /opt/dista/bin/dista
ENV LD_LIBRARY_PATH=/opt/dista/lib
ENTRYPOINT ["/opt/dista/bin/dista"]
