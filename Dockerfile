FROM fpco/stack-build-small:lts-18.28 AS builder

USER root
WORKDIR /root/vpf-tools

COPY src/ /root/vpf-tools/src/
COPY app/ /root/vpf-tools/app/
COPY test/ /root/vpf-tools/test/
COPY *.yaml /root/vpf-tools/

RUN stack build --copy-bins vpf-tools:vpf-class \
    && mkdir bin \
    && mv "$(stack path --local-bin)/vpf-class" bin/

FROM debian:buster

WORKDIR /opt/vpf-tools

COPY --from=builder /root/vpf-tools/bin/vpf-class /usr/local/bin/vpf-class

ARG DEBIAN_FRONTEND=noninteractive
ARG DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt update && apt install -y ca-certificates curl hmmer prodigal

VOLUME ["/opt/vpf-tools/vpf-data"]

# Update vpf-class data files if needed
ENV VPF_DATA_AUTOUPDATE=1
ENV VPF_CLASS_DATA_INDEX=/opt/vpf-tools/vpf-data/index.yaml
ENV VPF_TOOLS_CHMOD=1

COPY docker-entrypoint.sh /
CMD ["/bin/bash"]
ENTRYPOINT ["/docker-entrypoint.sh"]
