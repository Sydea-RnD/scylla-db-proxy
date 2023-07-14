#################
## build stage ##
#################
FROM rust:1-slim-bullseye AS builder
WORKDIR /code

# Download crates-io index and fetch dependency code.
# This step avoids needing to spend time on every build downloading the index
# which can take a long time within the docker context. Docker will cache it.

RUN USER=root cargo init
COPY Cargo.toml Cargo.toml
COPY cert.pem cert.pem
COPY key.pem key.pem
# COPY scylla_cert_test.crt scylla_cert.crt
COPY scylla_cert.crt scylla_cert.crt
RUN cargo fetch

# copy app files
COPY src src

RUN apt-get update
RUN apt-get install pkg-config libssl-dev -y

# compile app
RUN cargo build --release

###############
## run stage ##
###############
FROM debian:bullseye-slim

WORKDIR /app

# copy server binary from build stage
COPY --from=builder /code/target/release/scylla-rust-dbproxy scylla-rust-dbproxy
COPY --from=builder /code/cert.pem cert.pem
COPY --from=builder /code/key.pem key.pem
COPY --from=builder /code/scylla_cert.crt scylla_cert.crt

# set user to non-root unless root is required for your app
# USER 1001

# indicate what port the server is running on
EXPOSE 443

# run server
CMD [ "/app/scylla-rust-dbproxy" ]