#!/usr/bin/env bash
set -euo pipefail
# No network access. Provision the exact official source separately.
# Arguments: trusted official v0.1.1 source directory, qualification output directory.
source_dir=$(realpath "${1:?source directory required}")
mkdir -p "${2:?output directory required}"
out=$(realpath "$2")
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
(cd "$source_dir" && sha256sum --check "$here/source.sha256")
cc -O2 -std=gnu99 -fPIC -shared -DRNNOISE_BUILD -I"$source_dir/include" -I"$source_dir/src" \
    "$source_dir"/src/{denoise,rnn,rnn_data,rnn_reader,pitch,kiss_fft,celt_lpc}.c -lm -o "$out/librnnoise-v0.1.1.so"
cc -O2 -std=gnu99 -Wall -Wextra -Werror "$here/reference.c" -ldl -lm -o "$out/reference"
for fixture in wrong-frame null-state missing-symbol nonfinite; do
    case "$fixture" in
        wrong-frame) flag=-DFRAME_SIZE=481 ;;
        null-state) flag=-DNULL_STATE ;;
        missing-symbol) flag=-DMISSING_DESTROY ;;
        nonfinite) flag=-DNONFINITE_OUTPUT ;;
    esac
    cc -O2 -std=c99 -Wall -Wextra -Werror -fPIC -shared "$flag" "$here/abi-fixture.c" -o "$out/$fixture.so"
done
export PIPECAT_RNNOISE_LIBRARY="$out/librnnoise-v0.1.1.so"
export PIPECAT_RNNOISE_SHA256=$(sha256sum "$PIPECAT_RNNOISE_LIBRARY" | cut -d' ' -f1)
export PIPECAT_RNNOISE_FIXTURES="$out"
"$out/reference" "$PIPECAT_RNNOISE_LIBRARY" "$out"
printf 'library_sha256=%s\n' "$PIPECAT_RNNOISE_SHA256" > "$out/identity.txt"
sha256sum "$out"/*.pcm >> "$out/identity.txt"
# Run this script under the workspace's shared cargo-validation lock when shared.
cargo test --offline --locked -p pipecat-rnnoise --features native --test native -- --ignored --nocapture
