#!/bin/sh
# Checks a release archive as a 0.6 upgrader and a 0.7 one read it (ADR-0043, point 3.2).
#
#   scripts/check-archive.sh fleetringd-0.7.0-x86_64-unknown-linux-gnu.tar.gz 0.7.0 x86_64-unknown-linux-gnu
#
# The archive's `fleetringd-<version>-<target>/` directory must hold `fleetring` and
# `fleetringd` as regular files (a 0.6 upgrader extracts only a regular file named `fleetringd`;
# a hard link or a symbolic link fails it), identical byte for byte. Where this machine runs the
# target, each must print its own name and the version: `fleetring <version>` and
# `fleetringd <version>`. Exits 1 on the first failure, saying which.

set -eu

say() {
    printf 'check-archive.sh: %s\n' "$*" >&2
}

fail() {
    say "error: $*"
    exit 1
}

[ "$#" -eq 3 ] || {
    say "usage: check-archive.sh <archive.tar.gz> <version> <target>"
    exit 2
}
archive="$1"
version="$2"
target="$3"
dir="fleetringd-$version-$target"

[ -f "$archive" ] || fail "no archive at $archive"
case "$archive" in
    *.tar.gz) ;;
    *) fail "$archive isn't a .tar.gz; the Windows .zip holds two copies and isn't checked here" ;;
esac

# The target this machine runs natively, as install.sh names it, or nothing.
host_target() {
    arch=$(uname -m)
    case "$arch" in
        x86_64 | amd64) arch=x86_64 ;;
        aarch64 | arm64) arch=aarch64 ;;
        *) return 0 ;;
    esac
    case "$(uname -s)" in
        Linux) echo "$arch-unknown-linux-gnu" ;;
        Darwin) echo "$arch-apple-darwin" ;;
    esac
}

listing=$(tar -tvzf "$archive") || fail "can't list $archive"
for name in fleetring fleetringd; do
    path="$dir/$name"
    # The entry's line: the one with a field that is exactly its path.
    line=$(printf '%s\n' "$listing" | awk -v path="$path" '{ for (i = 1; i <= NF; i++) if ($i == path) { print; exit } }')
    [ -n "$line" ] || fail "$archive holds no $path"
    case "$line" in
        -*) ;;
        *) fail "$path isn't a regular file: $line" ;;
    esac
    case "$line" in
        *" link to "* | *" -> "*) fail "$path is a link: $line" ;;
    esac
done

tmp=$(mktemp -d 2> /dev/null || mktemp -d -t check-archive)
trap 'rm -rf "$tmp"' EXIT
trap 'exit 1' HUP INT TERM

tar -xzf "$archive" -C "$tmp" "$dir/fleetring" "$dir/fleetringd" || fail "can't extract $dir/fleetring and $dir/fleetringd"
for name in fleetring fleetringd; do
    [ -f "$tmp/$dir/$name" ] && [ ! -L "$tmp/$dir/$name" ] || fail "$dir/$name didn't extract as a regular file"
done
cmp -s "$tmp/$dir/fleetring" "$tmp/$dir/fleetringd" || fail "$dir/fleetring and $dir/fleetringd differ"
say "$dir/fleetring and $dir/fleetringd are regular files, identical byte for byte"

if [ "$(host_target)" != "$target" ]; then
    say "this machine doesn't run $target, so neither binary was run"
    exit 0
fi
for name in fleetring fleetringd; do
    printed=$("$tmp/$dir/$name" --version) || fail "$dir/$name --version failed"
    [ "$printed" = "$name $version" ] || fail "$dir/$name --version printed '$printed', not '$name $version'"
done
say "each prints its own name and $version"
