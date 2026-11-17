#!/bin/bash
# FR-097 (#147): the fork usability trial's kit. It prepares an isolated dev node with two
# databases of real-looking data, and switches the tight-disk configuration for step 6 on and
# off. It never touches your own ~/.fleetring: everything lives under $FR097_DIR.
#
#   bash experiments/fr097/kit.sh setup        build fleetring, start the dev node, fill the databases
#   bash experiments/fr097/kit.sh tight-disk   restart the node with a fork admission limit no disk meets
#   bash experiments/fr097/kit.sh normal       restart it with the limit removed
#   bash experiments/fr097/kit.sh teardown     stop the node and delete $FR097_DIR
#
# Then, in a new terminal: `. $FR097_DIR/env.sh` (default /tmp/fr097/env.sh) and follow
# TRIAL.md. Sizes: NOTES_MIB (default 300) and ARCHIVE_MIB (default 1500).
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
DIR=${FR097_DIR:-/tmp/fr097}
NOTES_MIB=${NOTES_MIB:-300}
ARCHIVE_MIB=${ARCHIVE_MIB:-1500}
PORT=${FR097_PORT:-7700}
BIN="$DIR/bin"
MARK="# fr097 tight-disk"
WAS="# fr097 set aside:"

# Run fleetring as the trial's isolated user: its own HOME, none of your FLEETRING_* variables.
fr() {
  env -u FLEETRING_CONTEXT -u FLEETRING_AGENT -u XDG_CONFIG_HOME -u XDG_DATA_HOME \
    HOME="$DIR/home" NO_COLOR=1 "$BIN/fleetring" "$@"
}

config() { echo "$DIR/home/.fleetring/fleetringd.toml"; }

restart() {
  fr dev stop > /dev/null 2>&1 || true
  fr dev -d --port "$PORT" > /dev/null
}

setup() {
  if [ -e "$DIR" ]; then
    echo "$DIR exists; run 'kit.sh teardown' first, or set FR097_DIR" >&2
    exit 1
  fi
  mkdir -p "$BIN" "$DIR/home"
  echo "building fleetring (release)..."
  cargo build --release -p fleetringd --manifest-path "$REPO/Cargo.toml" -q
  cp "$REPO/target/release/fleetring" "$BIN/"

  cat > "$DIR/env.sh" <<EOF
# Source this in the terminal you run the trial in: . $DIR/env.sh
unset FLEETRING_CONTEXT FLEETRING_AGENT XDG_CONFIG_HOME XDG_DATA_HOME
export HOME="$DIR/home"
export PATH="$BIN:\$PATH"
cd "$DIR"
echo "trial shell: \$(fleetring --version), HOME=\$HOME"
EOF

  echo "starting the dev node on port $PORT..."
  fr dev -d --port "$PORT" > /dev/null

  echo "filling notes (~$NOTES_MIB MiB of notes with text, tags and dates)..."
  fill_notes notes "$NOTES_MIB"
  echo "filling archive (~$ARCHIVE_MIB MiB: notes plus attachments)..."
  fill_notes archive 20
  fill_attachments archive "$((ARCHIVE_MIB - 20))"

  fr db list
  echo
  echo "Ready. Open a new terminal, run: . $DIR/env.sh"
  echo "Then follow experiments/fr097/TRIAL.md and write your notes in a copy of NOTES.md."
}

# notes(id, title, body, tags, created_at, updated_at): about 1 KiB a row of words drawn from a
# vocabulary, so the data reads like notes rather than random bytes. Batches stay well inside
# the statement deadline.
fill_notes() { # db mib
  local db=$1 rows=$(($2 * 1024)) batch=4000 lo=1 hi
  fr db create "$db" --description "Trial data for FR-097" > /dev/null
  fr db exec "$db" "CREATE TABLE words(w TEXT NOT NULL)" > /dev/null
  fr db exec "$db" "INSERT INTO words(w) VALUES $(vocabulary)" > /dev/null
  fr db exec "$db" "CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL, tags TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)" > /dev/null
  while [ "$lo" -le "$rows" ]; do
    hi=$((lo + batch - 1)); [ "$hi" -gt "$rows" ] && hi=$rows
    fr db exec "$db" "WITH RECURSIVE c(x) AS (SELECT $lo UNION ALL SELECT x + 1 FROM c WHERE x < $hi)
      INSERT INTO notes(id, title, body, tags, created_at, updated_at)
      SELECT x,
        (SELECT group_concat(w, ' ') FROM (SELECT w FROM words WHERE x > 0 ORDER BY random() LIMIT 6)),
        (SELECT group_concat(w, ' ') FROM (SELECT w FROM words WHERE x > 0 ORDER BY random() LIMIT 90))
          || '. ' ||
        (SELECT group_concat(w, ' ') FROM (SELECT w FROM words WHERE x > 0 ORDER BY random() LIMIT 70)) || '.',
        (SELECT group_concat(w, ',') FROM (SELECT w FROM words WHERE x > 0 ORDER BY random() LIMIT 3)),
        datetime('2024-01-01', '+' || (x * 97 % 63072000) || ' seconds'),
        datetime('2024-01-01', '+' || (x * 97 % 63072000 + abs(random() % 864000)) || ' seconds')
      FROM c" > /dev/null
    lo=$((hi + 1))
  done
  fr db exec "$db" "CREATE INDEX notes_by_created ON notes(created_at)" > /dev/null
}

# attachments(id, note_id, name, mime, data): 64 KiB blobs, so the database is large enough
# that a fork of it takes long enough to interrupt.
fill_attachments() { # db mib
  local db=$1 rows=$(($2 * 16)) batch=400 lo=1 hi
  fr db exec "$db" "CREATE TABLE attachments(id INTEGER PRIMARY KEY, note_id INTEGER NOT NULL REFERENCES notes(id), name TEXT NOT NULL, mime TEXT NOT NULL, data BLOB NOT NULL)" > /dev/null
  while [ "$lo" -le "$rows" ]; do
    hi=$((lo + batch - 1)); [ "$hi" -gt "$rows" ] && hi=$rows
    fr db exec "$db" "WITH RECURSIVE c(x) AS (SELECT $lo UNION ALL SELECT x + 1 FROM c WHERE x < $hi)
      INSERT INTO attachments(id, note_id, name, mime, data)
      SELECT x, 1 + x % 20480, 'scan-' || x || '.png', 'image/png', randomblob(65536) FROM c" > /dev/null
    lo=$((hi + 1))
  done
}

vocabulary() {
  local words="agenda budget customer deadline design draft estimate feedback goal hiring idea
    incident interview invoice kickoff launch meeting migration milestone notes onboarding outage
    plan priority proposal quarter release retro review risk roadmap schedule scope sprint status
    summary support task team ticket timeline travel update vendor workshop the a an and of to in
    for with on at by from about after before during over under between through against without
    we they she he it our their this that these those will should could must might can need wants
    discussed agreed decided reviewed shipped blocked moved fixed found asked shared sent booked
    called noted followed confirmed delayed approved rejected postponed escalated tested measured
    Monday Tuesday Wednesday Thursday Friday morning afternoon evening week month next last early
    late soon today tomorrow yesterday client server database backup report dashboard metrics
    alert login payment checkout search signup cache queue index schema query page form email"
  local out="" w
  for w in $words; do out="$out('$w'),"; done
  printf '%s' "${out%,}"
}

tight_disk() {
  local f; f=$(config)
  normal_config
  if grep -q '^\[fork\]' "$f"; then
    # The dev node writes its own small margin; set it aside rather than repeat the key.
    sed -i.bak -e "s/^admission_min_free_bytes[[:space:]]*=/$WAS &/" -e "/^\[fork\]/a\\
admission_min_free_bytes = 1125899906842624  $MARK
" "$f" && rm -f "$f.bak"
  else
    printf '\n[fork]  %s\nadmission_min_free_bytes = 1125899906842624  %s\n' "$MARK" "$MARK" >> "$f"
  fi
  restart
  echo "restarted with a fork admission limit of 1 PiB free: every new fork should be refused for disk"
}

normal_config() {
  local f; f=$(config)
  sed -i.bak -e "/$MARK/d" -e "s/^$WAS //" "$f" && rm -f "$f.bak"
}

normal() {
  normal_config
  restart
  echo "restarted with the default fork admission limits"
}

teardown() {
  fr dev stop > /dev/null 2>&1 || true
  rm -rf "$DIR"
  echo "removed $DIR"
}

case "${1:-}" in
  setup) setup ;;
  tight-disk) tight_disk ;;
  normal) normal ;;
  teardown) teardown ;;
  *) sed -n '2,12p' "$0" >&2; exit 2 ;;
esac
