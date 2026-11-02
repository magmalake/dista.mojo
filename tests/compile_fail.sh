#!/bin/bash
# Programs that must not compile, each with the error it must give: dista
# turns some mistakes into compile errors, and this keeps them errors.
set -uo pipefail
mkdir -p build
status=0
expect_error() {
  local file=$1 want=$2
  local out
  if out=$(mojo build -I src ${DEP_INC:--I src} "$file" -o build/compile-fail 2>&1); then
    echo "FAIL: $file compiled"; status=1; return
  fi
  if ! grep -qF "$want" <<< "$out"; then
    echo "FAIL: $file failed without \"$want\":"; echo "$out" | tail -5; status=1; return
  fi
  echo "ok: $file"
}
expect_error tests/compile_fail/nested_ref.mojo "dista can't send an ObjectRef inside a value"
[ $status = 0 ] && echo "PASS compile-fail: every program was refused with its error"
exit $status
