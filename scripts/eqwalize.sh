#!/bin/sh
# eqWAlizer (WhatsApp's gradual type checker, via ELP) over src/ only.
#
# Tests deliberately pass mistyped inputs to assert the `badarg` misuse path,
# so test findings are expected and not gated. `elp eqwalize-all` exits 0 even
# when it reports findings, and non-zero only when it could not run (e.g. no
# Java 11+ on PATH) — both cases are handled so a broken run never passes.
set -eu
out="$(elp eqwalize-all --format json 2>/dev/null)" || {
    echo "eqwalize: elp failed to run (is Java 11+ on PATH?)" >&2
    exit 2
}
found="$(printf '%s\n' "$out" | grep '^{' | jq -r 'select(.path | startswith("src/"))
    | "\(.path):\(.line): \(.name)\n    \(.original)"' || true)"
if [ -n "$found" ]; then
    printf '%s\n' "$found"
    exit 1
fi
echo "eqwalize: src/ is clean"
