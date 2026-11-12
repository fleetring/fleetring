# The FR-108 spike's agent image: the OpenShell default base plus the tools the hostile-agent
# script uses (curl for Hrana over HTTP, python3 for raw sockets, tmux and sqlite3 as targets,
# netcat and dig for exfiltration attempts). No FleetRing binary, no credentials.
FROM nvcr.io/nvidia/base/ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl python3 sqlite3 tmux \
      netcat-openbsd dnsutils procps \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --user-group --no-create-home --home-dir /sandbox agent
# An account of its own with no supplementary groups: `USER 1000` would run as the base image's
# `ubuntu` account, which is in the sudo and adm groups.
USER 10001:10001
WORKDIR /sandbox
