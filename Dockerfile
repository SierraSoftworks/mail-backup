# NOTE: This Dockerfile depends on you building the mail-backup binary first.
# It will then package that binary into the image, and use that as the entrypoint.
# This means that running `docker build` is not a repeatable way to build the same
# image, but the benefit is much faster cross-platform builds; a net win.
FROM ubuntu:24.04

LABEL org.opencontainers.image.source=https://github.com/SierraSoftworks/mail-backup
LABEL org.opencontainers.image.description="Backup your JMAP mailboxes to a local git repository automatically"
LABEL org.opencontainers.image.licenses=MIT

RUN apt-get update && \
    apt-get install -y openssl ca-certificates git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD ./mail-backup /usr/local/bin/mail-backup

ENTRYPOINT ["/usr/local/bin/mail-backup"]
