#!/bin/bash
# What the gateway keeps on the host, on Docker: its state, and whether agents' tokens appear in
# it in the clear. Writes results/trusted-base.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/trusted-base.txt"
exec > "$OUT" 2>&1
step "files under the gateway's HOME (it was redirected to <work>/os/home)"
find "$WORK/os/home" -type f -exec ls -la {} \; | awk '{print $1, $5, $NF}'
step "agent A's token, searched for in the clear in every one of those files"
T=$(tr -d '\n' < "$DEV/agents/a/token")
grep -r -l -F "$T" "$WORK/os/home" 2>/dev/null || echo "not found in the clear"
step "the operator's client credential the CLI uses (an administrator credential)"
ls -la "$WORK/os/home/.config/openshell/gateways/"*/mtls 2>&1 | awk '{print $1, $NF}'
scrub < "$OUT" > "$RES/trusted-base.txt"
