#!/bin/sh
# bench/sizing.mojo under each setting, each in a process of its own (the
# peak memory it prints is per process): a slot per core set by hand, the
# slots a grant's memory fits set by hand, and `auto` from the same grant,
# with and without DISTA_TASK_MEMORY_MB. Without, it says whether the worker
# kept inside the grant, and exits 1 if not.
#
# Usage: bench/sizing.sh [grant memory MB, default 4096]
set -eu
mem=${1:-4096}
cpus=$(getconf _NPROCESSORS_ONLN)
fit=$((mem / 512))
[ "$fit" -ge 1 ] || fit=1
[ "$fit" -le "$cpus" ] || fit=$cpus
scripts/mojo-build.sh -I src $DEP_INC bench/sizing.mojo -o build/bench-sizing
echo "memory-heavy mix (64 and 512 MB tasks), $cpus cores, grant ${mem} MB"
env -u MORCA_GRANT_CPUS -u MORCA_GRANT_MEMORY_MB DISTA_WORKER_SLOTS="$cpus" \
    SIZING_LABEL="hand-set slots=$cpus  " ./build/bench-sizing
env -u MORCA_GRANT_CPUS -u MORCA_GRANT_MEMORY_MB DISTA_WORKER_SLOTS="$fit" \
    SIZING_LABEL="hand-set slots=$fit   " ./build/bench-sizing
line=$(DISTA_WORKER_SLOTS=auto MORCA_GRANT_CPUS="$cpus" \
    MORCA_GRANT_MEMORY_MB="$mem" SIZING_LABEL="auto, no size       " \
    ./build/bench-sizing | grep wall_s)
peak=${line##*worker_peak_mb=}
if [ "$peak" -le "$mem" ]; then inside=yes; else inside=NO; fi
echo "$line inside_grant=$inside"
DISTA_WORKER_SLOTS=auto MORCA_GRANT_CPUS="$cpus" MORCA_GRANT_MEMORY_MB="$mem" \
    DISTA_TASK_MEMORY_MB=512 \
    SIZING_LABEL="auto, task 512 MB   " ./build/bench-sizing
[ "$inside" = yes ]
