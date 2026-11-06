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
# the binary to PREFIX/bin/fleetring, with PREFIX/bin/fleetringd a link to it. A release before
# 0.7 installs as PREFIX/bin/fleetringd alone, as it always did. Supported: Linux (glibc 2.36 or newer) and macOS, each on x86_64
# and ARM64. On Windows, download the .zip from the release page.

set -eu

repo="fleetring/fleetring"
version="${FLEETRINGD_VERSION:-}"
prefix="${FLEETRINGD_PREFIX:-}"
base_url="${FLEETRINGD_BASE_URL:-}"

usage() {
    cat <<EOF
Installs fleetring from a GitHub release, and fleetringd, its name before 0.7, as a link
to it.

Usage: install.sh [--version VERSION] [--prefix DIR] [--base-url URL]

Options:
  --version VERSION  The version to install, such as 0.1.0. Default: the newest release.
                     Also read from FLEETRINGD_VERSION.
  --prefix DIR       Installs the binary to DIR/bin. Default: /usr/local as root,
                     \$HOME/.local otherwise. Also read from FLEETRINGD_PREFIX.
  --base-url URL     Downloads the archive and SHA256SUMS from URL instead of the GitHub
                     release, for a mirror. Needs --version. Also read from FLEETRINGD_BASE_URL.
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
else
    say "installed $("$bin_dir/fleetringd" --version) to $bin_dir/fleetringd"
    # A release before 0.7 knows only `fleetringd`. A `fleetring` left by a later install would
    # run another release, so it follows `fleetringd` instead.
    if [ -e "$bin_dir/fleetring" ] || [ -L "$bin_dir/fleetring" ]; then
        ln -sf fleetringd "$bin_dir/.fleetring.new" \
            || fail "can't write to $bin_dir; pass --prefix, or run as a user who can write there"
        mv -f "$bin_dir/.fleetring.new" "$bin_dir/fleetring"
        say "$bin_dir/fleetring is now a link to fleetringd"
    fi
fi
case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *) say "$bin_dir is not on your PATH; add it, or run $bin_dir/$binary" ;;
esac
