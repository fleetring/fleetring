#!/bin/bash
# OpenShell's own approval modes, on Docker: a sandbox created with --approval-mode auto, first
# with no gateway-wide value, then with `manual` set gateway-wide. Each tries
# https://example.com six times, 5 s apart. Writes results/approval.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/approval.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1
printf 'version: 1\nfilesystem_policy:\n  include_workdir: true\n  read_only: [/usr, /lib, /bin, /etc]\n  read_write: [/tmp]\n' > "$WORK/run/policy-c.yaml"
try() { # sandbox
  for i in 1 2 3 4 5 6; do
    ax "$1" 'echo "$(date -u +%s) attempt: HTTP $(curl -s -m 5 -o /dev/null -w %{http_code} https://example.com)"'
    sleep 5
  done
  osh openshell rule get "$1" 2>&1 | grep -E "Status|Rule:|Prover|Endpoints|Binaries"
  osh openshell settings get "$1" 2>&1 | grep -i approval
}

step "no gateway-wide value; sandbox created with --approval-mode auto"
osh openshell settings delete --global --key proposal_approval_mode --yes 2>&1 | tail -1
osh openshell sandbox create --name agent-c --from fr108-agent:spike --policy "$WORK/run/policy-c.yaml" --approval-mode auto --detach -- sleep infinity 2>&1 | tail -1
try agent-c
osh openshell sandbox delete agent-c 2>&1 | tail -1

step "gateway-wide manual; sandbox created with --approval-mode auto"
osh openshell settings set --global --key proposal_approval_mode --value manual --yes 2>&1 | tail -1
osh openshell sandbox create --name agent-c2 --from fr108-agent:spike --policy "$WORK/run/policy-c.yaml" --approval-mode auto --detach -- sleep infinity 2>&1 | tail -1
try agent-c2
osh openshell sandbox delete agent-c2 2>&1 | tail -1

step "drafts OpenShell made from A's denied connections during the other runs (manual: pending, never applied)"
osh openshell rule get agent-a --status pending 2>&1 | grep -E "Status|Rule:|Endpoints|Binaries" | head -24

scrub < "$OUT" > "$RES/approval.txt"
