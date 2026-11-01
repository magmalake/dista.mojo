#!/bin/bash
# The short AWS gate; now `bench/cloud/bench gate`, kept as a name.
exec "$(dirname "$0")/bench" gate "$@"
