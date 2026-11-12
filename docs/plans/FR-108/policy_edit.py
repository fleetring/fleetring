"""Edit a sandbox base policy, as `openshell policy get --base` prints it, for one experiment.

    python3 -I policy_edit.py <in> <out> widen          router rule allows POST /db/** (scopes alone guard)
    python3 -I policy_edit.py <in> <out> unbind         drop the router endpoint's credential_binding
    python3 -I policy_edit.py <in> <out> bin <path>     add a binary to the router rule
    python3 -I policy_edit.py <in> <out> rule <file>    append the rule(s) in <file> to network_policies

Text edits on the known layout, so the result keeps every path OpenShell added at start."""
import re
import sys

src, dst, mode = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(src, encoding="utf-8").read()
if mode == "widen":
    start = text.index("        rules:")
    end = text.index("        allowed_ips:")
    text = text[:start] + "        rules:\n          - allow:\n              method: POST\n              path: /db/**\n" + text[end:]
elif mode == "unbind":
    text = re.sub(r"        credential_binding:\n          provider: \S+\n", "", text)
elif mode == "bin":
    text = text.replace("      - path: /usr/bin/curl", "      - path: /usr/bin/curl\n      - path: " + sys.argv[4], 1)
elif mode == "rule":
    text = text.rstrip("\n") + "\n" + open(sys.argv[4], encoding="utf-8").read()
else:
    sys.exit("unknown mode " + mode)
open(dst, "w", encoding="utf-8").write(text if text.endswith("\n") else text + "\n")
