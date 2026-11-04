#!/bin/sh
# `mojo build`, reusing the binary of an identical earlier build.
#
# Several tests build the same binaries (build/dista three times, the smoke
# test and the restart driver twice each), and scripts/build-tests.sh builds
# the test binaries at once ahead of the tasks that run them.
# This keys a build on everything it can read and keeps each result under
# build/.mojo-build/, so a second build with the same key is a copy:
#
# - the arguments but -o's (sources, -I, -D and the rest), and the
#   environment's packages (the file names in $CONDA_PREFIX/conda-meta name
#   each one's version and build);
# - the name and contents of every .mojo, .mojoc and .mojopkg file under the
#   -I directories and under each source's directory, .git, .pixi and build
#   left out.
#
# A build that fails keeps nothing. Entries unused for two days are removed;
# rm -rf build/.mojo-build clears them all. MOJO_BUILD_CACHE=0 always builds.
#
# Usage: scripts/mojo-build.sh <mojo build arguments...> -o <out>
set -eu

out=
prev=
dirs=
for a in "$@"; do
    case "$prev" in
    -o) out=$a ;;
    -I) dirs="$dirs $a" ;;
    esac
    case "$a" in
    *.mojo) dirs="$dirs $(dirname "$a")" ;;
    esac
    prev=$a
done
if [ -z "$out" ] || [ "${MOJO_BUILD_CACHE:-1}" = 0 ]; then
    exec mojo build "$@"
fi

if command -v sha256sum > /dev/null 2>&1; then
    sha() { sha256sum | cut -c1-64; }
else
    sha() { shasum -a 256 | cut -c1-64; }
fi

list=$(mktemp "${TMPDIR:-/tmp}/mojo-build.XXXXXX")
trap 'rm -f "$list"' EXIT INT TERM
# shellcheck disable=SC2086
find $dirs \( -name .git -o -name .pixi -o -name build \) -prune -o -type f \
    \( -name '*.mojo' -o -name '*.mojoc' -o -name '*.mojopkg' \) -print \
    | LC_ALL=C sort -u > "$list"
key=$({
    # The arguments but -o's: one build serves every name it's copied to.
    skip=
    for a in "$@"; do
        if [ -n "$skip" ]; then
            skip=
        elif [ "$a" = -o ]; then
            skip=1
        else
            printf '%s\n' "$a"
        fi
    done
    ls "${CONDA_PREFIX:-}/conda-meta" 2> /dev/null || true
    cat "${CONDA_PREFIX:-}/conda-meta/.pixi-environment-fingerprint" 2> /dev/null || true
    cat "$list"
    tr '\n' '\0' < "$list" | xargs -0 cat
} | sha)

cache=build/.mojo-build
hit=$cache/$key
mkdir -p "$cache" "$(dirname "$out")"
if [ -f "$hit" ]; then
    # A copy moved into place: a binary that's running keeps its old file.
    cp "$hit" "$out.tmp.$$"
    mv -f "$out.tmp.$$" "$out"
    touch "$hit"
    echo "mojo-build: $out reused" >&2
    exit 0
fi

start=$(date +%s)
mojo build "$@"
cp "$out" "$hit.tmp.$$"
mv -f "$hit.tmp.$$" "$hit"
echo "mojo-build: $out built in $(($(date +%s) - start))s" >&2
find "$cache" -type f -mtime +2 -delete 2> /dev/null || true
