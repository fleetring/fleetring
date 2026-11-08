#!/bin/sh
# Installs fleetring from a GitHub release (FR-034), with `fleetringd`, its name before 0.7, as a
# link to it until 1.0 (ADR-0043).
#
#   curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/fleetring/fleetring/main/scripts/install.sh \
#     | sh -s -- --version 0.1.0 --prefix /usr/local
#
# It picks the archive for this OS and CPU, checks it against the release's SHA256SUMS (after
# verifying that file's cosign signature, when cosign is installed), and copies
# the binary to PREFIX/bin/fleetring, with PREFIX/bin/fleetringd a link to it, and the man pages,
# when the release has them (0.7 on), to PREFIX/share/man/man1. A release before 0.7 installs as
# PREFIX/bin/fleetringd alone, as it always did.
#
# From 0.7 it also makes PREFIX/bin/fleet, a short name, a link to fleetring, unless --no-alias
# (or FLEETRINGD_NO_ALIAS=1) says not to. It leaves alone a PREFIX/bin/fleet that isn't a link to
# fleetring or fleetringd, and adds nothing if another `fleet` is on the PATH (Rancher Fleet and
# JetBrains Fleet use the name), and says so. It never runs a `fleet` to find out.
#
# Supported: Linux (glibc 2.36 or newer) and macOS, each on x86_64
# and ARM64. On Windows, download the .zip from the release page.

set -eu

repo="fleetring/fleetring"
version="${FLEETRINGD_VERSION:-}"
prefix="${FLEETRINGD_PREFIX:-}"
base_url="${FLEETRINGD_BASE_URL:-}"
no_alias="${FLEETRINGD_NO_ALIAS:-}"

usage() {
    cat <<EOF
Installs fleetring from a GitHub release, with fleetringd, its name before 0.7, and fleet, a
short name, as links to it.

Usage: install.sh [--version VERSION] [--prefix DIR] [--base-url URL] [--no-alias]

Options:
  --version VERSION  The version to install, such as 0.1.0. Default: the newest release.
                     Also read from FLEETRINGD_VERSION.
  --prefix DIR       Installs the binary to DIR/bin, and the man pages to DIR/share/man/man1.
                     Default: /usr/local as root, \$HOME/.local otherwise. Also read from
                     FLEETRINGD_PREFIX.
  --base-url URL     Downloads the archive and SHA256SUMS from URL instead of the GitHub
                     release, for a mirror. Needs --version. Also read from FLEETRINGD_BASE_URL.
  --no-alias         Doesn't add DIR/bin/fleet, a link to fleetring (0.7 on). It is added only if
                     DIR/bin/fleet is absent or already a link to fleetring, and no other fleet is
                     on your PATH. Also set by FLEETRINGD_NO_ALIAS=1.
  -h, --help         Prints this help.
EOF
}

say() {
    printf 'install.sh: %s\n' "$*" >&2
}

fail() {
    say "error: $*"
    exit 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            [ "$#" -ge 2 ] || fail "--version needs a value"
            version="$2"
            shift 2
            ;;
        --version=*)
            version="${1#*=}"
            shift
            ;;
        --prefix)
            [ "$#" -ge 2 ] || fail "--prefix needs a value"
            prefix="$2"
            shift 2
            ;;
        --prefix=*)
            prefix="${1#*=}"
            shift
            ;;
        --base-url)
            [ "$#" -ge 2 ] || fail "--base-url needs a value"
            base_url="$2"
            shift 2
            ;;
        --base-url=*)
            base_url="${1#*=}"
            shift
            ;;
        --no-alias)
            no_alias=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            fail "unknown argument: $1"
            ;;
    esac
done

# Downloads $1 to the file $2, or to standard output when $2 is `-`.
download() {
    if command -v curl > /dev/null 2>&1; then
        curl --proto '=https,file' --tlsv1.2 -fsSL -o "$2" "$1"
    elif command -v wget > /dev/null 2>&1; then
        wget -q -O "$2" "$1"
    else
        fail "needs curl or wget"
    fi
}

sha256() {
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    elif command -v shasum > /dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    elif command -v openssl > /dev/null 2>&1; then
        openssl dgst -sha256 "$1" | sed 's/^.*= *//'
    else
        fail "needs sha256sum, shasum, or openssl to verify the download"
    fi
}

detect_target() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$arch" in
        x86_64 | amd64) arch=x86_64 ;;
        aarch64 | arm64) arch=aarch64 ;;
        *) fail "no release build for the $arch architecture; build from source with cargo" ;;
    esac
    case "$os" in
        Linux)
            if ldd --version 2>&1 | grep -qi musl; then
                fail "release builds need glibc, and this system uses musl; use the container image ghcr.io/fleetring/fleetring, or build from source"
            fi
            echo "$arch-unknown-linux-gnu"
            ;;
        Darwin)
            # A shell under Rosetta reports x86_64 on Apple Silicon; prefer the native build.
            if [ "$arch" = x86_64 ] && [ "$(sysctl -n hw.optional.arm64 2> /dev/null || echo 0)" = 1 ]; then
                arch=aarch64
            fi
            echo "$arch-apple-darwin"
            ;;
        MINGW* | MSYS* | CYGWIN* | Windows_NT)
            fail "on Windows, download fleetringd-<version>-x86_64-pc-windows-msvc.zip from https://github.com/$repo/releases"
            ;;
        *) fail "no release build for $os; build from source with cargo" ;;
    esac
}

# The newest final release: the first `fleetringd-vX.Y.Z` tag GitHub lists. Prereleases (with a
# `-` suffix) and the `fleetring` crate's tags don't match.
latest_version() {
    download "https://api.github.com/repos/$repo/releases?per_page=100" - \
        | grep -o '"tag_name": *"fleetringd-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"' \
        | head -n 1 \
        | sed 's/.*"fleetringd-v\([^"]*\)"/\1/'
}

# The `fleet` alias, a link to `fleetring` (ADR-0043, amendment of 2026-11-07), for a release that
# has `fleetring`. It must never replace or be shadowed by someone else's `fleet` (Rancher
# Fleet's CLI, JetBrains Fleet's launcher), so it is made only if PREFIX/bin/fleet is absent or
# already a link to fleetring or fleetringd, and no other `fleet` is on the PATH. Nothing here
# runs a `fleet`: links are read with readlink, and anything else is only looked at.
link_is_ours() {
    [ -L "$1" ] || return 1
    case "$(readlink "$1" 2> /dev/null)" in
        fleetring | fleetringd | */fleetring | */fleetringd) return 0 ;;
        *) return 1 ;;
    esac
}

add_alias() {
    alias_path="$bin_dir/fleet"
    if [ -e "$alias_path" ] || [ -L "$alias_path" ]; then
        if ! link_is_ours "$alias_path"; then
            say "left $alias_path alone: it isn't a link to fleetring"
            return 0
        fi
    fi
    other=$(command -v fleet 2> /dev/null || true)
    case "$other" in
        /*)
            if ! [ "$other" -ef "$alias_path" ] && ! link_is_ours "$other"; then
                say "another fleet is on your PATH at $other; not adding the fleet alias"
                return 0
            fi
            ;;
    esac
    ln -sf fleetring "$bin_dir/.fleet.new" \
        || fail "can't write to $bin_dir; pass --prefix, or run as a user who can write there"
    mv -f "$bin_dir/.fleet.new" "$alias_path"
    say "installed $alias_path, a link to fleetring (--no-alias skips it)"
}

target=$(detect_target)

case "$version" in
    "") ;;
    fleetringd-v*) version="${version#fleetringd-v}" ;;
    v*) version="${version#v}" ;;
esac
if [ -z "$version" ]; then
    [ -z "$base_url" ] || fail "--base-url needs --version"
    version=$(latest_version) || true
    [ -n "$version" ] || fail "can't find the newest fleetring release; pass --version"
fi
if [ -z "$base_url" ]; then
    base_url="https://github.com/$repo/releases/download/fleetringd-v$version"
fi

if [ -z "$prefix" ]; then
    if [ "$(id -u)" = 0 ]; then
        prefix=/usr/local
    else
        prefix="$HOME/.local"
    fi
fi
bin_dir="$prefix/bin"
man_dir="$prefix/share/man/man1"

name="fleetringd-$version-$target"
archive="$name.tar.gz"

tmp=$(mktemp -d 2> /dev/null || mktemp -d -t fleetring)
trap 'rm -rf "$tmp"' EXIT
trap 'exit 1' HUP INT TERM

say "downloading fleetring $version for $target"
download "$base_url/$archive" "$tmp/$archive" || fail "can't download $base_url/$archive"
download "$base_url/SHA256SUMS" "$tmp/SHA256SUMS" || fail "can't download $base_url/SHA256SUMS"

# SHA256SUMS is signed keylessly by the release workflow (FR-048) with cosign v3, which writes a
# Sigstore bundle. Cosign older than 2.4.2 can't check that bundle (2.4.0 and 2.4.1 fail with
# "bundle does not contain cert"), so it counts as not installed. With a new enough cosign, the
# signature must verify before any checksum is trusted; without one, say so and carry on.
cosign_min="2.4.2"
cosign_min_num=2004002
cosign_state=missing
if command -v cosign > /dev/null 2>&1; then
    cosign_state=old
    # "cosign version" prints a line such as "GitVersion:    v2.4.3"; this leaves "2 4 3", which
    # is folded into MAJOR*1000000 + MINOR*1000 + PATCH.
    cosign_ver=$(cosign version 2> /dev/null \
        | sed -n 's/^GitVersion:[[:space:]]*v\{0,1\}\([0-9][0-9]*\)\.\([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1 \2 \3/p' | head -n 1)
    if [ -n "$cosign_ver" ]; then
        # shellcheck disable=SC2086 # split "MAJOR MINOR PATCH" into three words
        cosign_num=$(printf '%s\n' $cosign_ver | awk '{ n = n * 1000 + $1 } END { print n }')
        if [ "$cosign_num" -ge "$cosign_min_num" ]; then
            cosign_state=ok
        fi
    fi
fi
case "$cosign_state" in
    ok)
        download "$base_url/SHA256SUMS.sigstore.json" "$tmp/SHA256SUMS.sigstore.json" \
            || fail "can't download $base_url/SHA256SUMS.sigstore.json, so the signature can't be verified"
        cosign verify-blob \
            --bundle "$tmp/SHA256SUMS.sigstore.json" \
            --certificate-identity-regexp "^https://github.com/$repo/.github/workflows/release.yml@refs/tags/fleetringd-v" \
            --certificate-oidc-issuer https://token.actions.githubusercontent.com \
            "$tmp/SHA256SUMS" > /dev/null \
            || fail "the signature on SHA256SUMS doesn't verify; not installing"
        say "verified the cosign signature on SHA256SUMS"
        ;;
    old)
        say "the installed cosign is too old to check the SHA256SUMS signature (needs $cosign_min or newer); to check it, upgrade cosign (https://docs.sigstore.dev/cosign/system_config/installation/)"
        ;;
    *)
        say "cosign not found, so the SHA256SUMS signature wasn't checked; to check it, install cosign (https://docs.sigstore.dev/cosign/system_config/installation/)"
        ;;
esac

expected=$(awk -v file="$archive" '$2 == file || $2 == "*" file { print $1 }' "$tmp/SHA256SUMS")
[ -n "$expected" ] || fail "SHA256SUMS lists no checksum for $archive"
actual=$(sha256 "$tmp/$archive")
if [ "$actual" != "$expected" ]; then
    fail "checksum mismatch for $archive: expected $expected, got $actual"
fi
say "verified the SHA-256 checksum"

tar -xzf "$tmp/$archive" -C "$tmp"
# From 0.7 the archive holds `fleetring` (and `fleetringd`, for 0.6 upgraders); before 0.7, only
# `fleetringd`. Either may be a link from 0.8 on; `[ -f ]` and `cp` follow it.
if [ -f "$tmp/$name/fleetring" ]; then
    binary=fleetring
elif [ -f "$tmp/$name/fleetringd" ]; then
    binary=fleetringd
else
    fail "$archive holds neither $name/fleetring nor $name/fleetringd"
fi

mkdir -p "$bin_dir" || fail "can't create $bin_dir; pass --prefix, or run as a user who can write there"
# Copies next to the destination, then renames, so a running binary is never overwritten in
# place.
cp "$tmp/$name/$binary" "$bin_dir/.$binary.new" \
    || fail "can't write to $bin_dir; pass --prefix, or run as a user who can write there"
chmod 0755 "$bin_dir/.$binary.new"
mv -f "$bin_dir/.$binary.new" "$bin_dir/$binary"

if [ "$binary" = fleetring ]; then
    # `fleetringd` is a link to `fleetring` until 1.0, replaced atomically, so whatever ran
    # `fleetringd` (a service unit, a script) runs this binary.
    ln -sf fleetring "$bin_dir/.fleetringd.new" \
        || fail "can't write to $bin_dir; pass --prefix, or run as a user who can write there"
    mv -f "$bin_dir/.fleetringd.new" "$bin_dir/fleetringd"
    say "installed $("$bin_dir/fleetring" --version) to $bin_dir/fleetring (also as fleetringd)"
    if [ "$no_alias" = 1 ]; then
        say "not adding the fleet alias: --no-alias"
    else
        add_alias
    fi
else
    say "installed $("$bin_dir/fleetringd" --version) to $bin_dir/fleetringd"
    # A release before 0.7 knows only `fleetringd`. A `fleetring` left by a later install would
    # run another release, so it follows `fleetringd` instead: if it is a link, or a binary that
    # names itself fleetring or fleetringd. Anything else named `fleetring` isn't ours to replace.
    if [ -L "$bin_dir/fleetring" ]; then
        ours=yes
    elif [ -f "$bin_dir/fleetring" ]; then
        case "$("$bin_dir/fleetring" --version 2> /dev/null | head -n 1)" in
            "fleetring "* | "fleetringd "*) ours=yes ;;
            *) ours=no ;;
        esac
    else
        ours=none
    fi
    case "$ours" in
        yes)
            ln -sf fleetringd "$bin_dir/.fleetring.new" \
                || fail "can't write to $bin_dir; pass --prefix, or run as a user who can write there"
            mv -f "$bin_dir/.fleetring.new" "$bin_dir/fleetring"
            say "$bin_dir/fleetring is now a link to fleetringd"
            ;;
        no)
            say "left $bin_dir/fleetring alone: it isn't a fleetring binary"
            ;;
    esac
fi
# The man pages, one per command, from 0.7 on. The binary is installed by now, so a failure here
# warns rather than fails.
if [ -d "$tmp/$name/man" ]; then
    pages=0
    if mkdir -p "$man_dir" 2> /dev/null; then
        for page in "$tmp/$name/man"/*.1; do
            [ -f "$page" ] || continue
            base=$(basename "$page")
            if cp "$page" "$man_dir/.$base.new" 2> /dev/null && chmod 0644 "$man_dir/.$base.new" \
                && mv -f "$man_dir/.$base.new" "$man_dir/$base"; then
                pages=$((pages + 1))
            else
                rm -f "$man_dir/.$base.new"
                say "warning: can't write to $man_dir, so the man pages weren't all installed; they are in the archive's man/"
                break
            fi
        done
        if [ "$pages" -gt 0 ]; then
            say "installed $pages man pages to $man_dir (man fleetring)"
        fi
    else
        say "warning: can't create $man_dir, so the man pages weren't installed; they are in the archive's man/"
    fi
fi

case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) say "$bin_dir is not on your PATH; add it, or run $bin_dir/$binary" ;;
esac
