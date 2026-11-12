"""Scrub a transcript on stdin: replace each literal from the map file, longest first, and strip
terminal colour codes. The map is tab-separated: literal, replacement."""
import re
import sys

pairs = []
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        line = line.rstrip("\n")
        if "\t" in line:
            literal, replacement = line.split("\t", 1)
            if literal:
                pairs.append((literal, replacement))
pairs.sort(key=lambda p: -len(p[0]))
text = sys.stdin.read()
text = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", text)
for literal, replacement in pairs:
    text = text.replace(literal, replacement)
# Long result rows (a SELECT over the probe loop's inserts) add nothing past their start.
lines = [
    line if len(line) <= 1500 else f"{line[:600]} … ({len(line)} characters; truncated)"
    for line in text.split("\n")
]
sys.stdout.write("\n".join(lines))
