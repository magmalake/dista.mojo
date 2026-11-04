#!/bin/sh
# Builds test binaries several at once, with the arguments their pixi tasks
# build them with, so each task's own build is a copy (scripts/mojo-build.sh).
# One `mojo build` keeps under two cores busy; this runs one per two cores.
#
# Usage: scripts/build-tests.sh [source.mojo...]
# With no sources: every tests/test_*.mojo the default environment builds
# (not the data environment's Parquet and Iceberg tests), and cli/main.mojo,
# which three of the shell tests build.
set -eu
if [ "$#" -eq 0 ]; then
    for f in tests/test_*.mojo; do
        case "$f" in
        tests/test_parquet_tasks.mojo | tests/test_iceberg_scan.mojo) ;;
        *) set -- "$@" "$f" ;;
        esac
    done
    set -- "$@" cli/main.mojo
fi
cores=$(getconf _NPROCESSORS_ONLN 2> /dev/null || echo 2)
jobs=$((cores / 2 + 1))
mkdir -p build
# shellcheck disable=SC2016
printf '%s\n' "$@" | xargs -n 1 -P "$jobs" sh -c '
    case "$1" in
    cli/main.mojo) exec scripts/mojo-build.sh -I src ${DEP_INC:--I src} "$1" -o build/dista ;;
    *) exec scripts/mojo-build.sh -I src $DEP_INC "$1" -o "build/$(basename "$1" .mojo | tr _ -)" ;;
    esac' sh
