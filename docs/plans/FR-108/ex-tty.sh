#!/bin/bash
# Row 3 with a real terminal, on Docker: `openshell sandbox connect` under a pseudo-terminal
# (macOS `script`). Writes results/tty.txt.
set -u
. "$(dirname "$0")/lib.sh"
OUT="$WORK/raw/tty.txt"
osh openshell gateway select fr108 > /dev/null
exec > "$OUT" 2>&1

step "an interactive session in A: its terminal, other terminals, and TIOCSTI into its own"
printf '%s\n' \
  'tty; ls -la /dev/pts 2>&1 | head; ls -la /dev/pts/0 /dev/pts/1 2>&1' \
  'python3 -c "import fcntl,termios,os; fd=os.open(os.ttyname(0), os.O_RDWR); [fcntl.ioctl(fd, termios.TIOCSTI, c) for c in [b\"i\",b\"d\",b\"\\n\"]]; print(\"TIOCSTI accepted\")" 2>&1' \
  'for p in /dev/pts/[0-9]*; do (echo hello > $p) 2>&1 | sed "s|^|$p: |" || true; done' \
  'exit' > "$WORK/raw/tty-input"
echo "(sandbox connect under a scripted pseudo-terminal printed nothing in an earlier attempt, so"
echo " the same commands run through sandbox exec --tty, which gives the command a real PTY)"
osh openshell sandbox exec --name agent-a --tty -- sh -c "$(grep -v '^exit$' "$WORK/raw/tty-input")" 2>&1 | tr -d '\r' | head -40

scrub < "$OUT" > "$RES/tty.txt"
