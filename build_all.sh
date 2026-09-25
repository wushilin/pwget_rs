#!/usr/bin/env bash
set -euo pipefail

# Build release binaries for the supported distribution targets. Cross-target
# C dependencies (notably ring) need the matching SDK/toolchain/sysroot installed.
targets=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  x86_64-unknown-freebsd
  x86_64-unknown-linux-musl
  aarch64-unknown-linux-musl
)

command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
command -v rustup >/dev/null || { echo "rustup is required" >&2; exit 1; }
stable_cargo=$(rustup which cargo --toolchain stable)
stable_rustc=$(rustup which rustc --toolchain stable)

rustup target add "${targets[@]}"
mkdir -p dist
failed=()

for target in "${targets[@]}"; do
  echo "==> Building pwget for ${target}"
  if [[ "$target" == *-unknown-linux-musl || "$target" == *-unknown-freebsd ]] && command -v cross >/dev/null; then
    build_status=0
    cross build --locked --release --target "$target" || build_status=$?
  else
    build_status=0
    RUSTC="$stable_rustc" "$stable_cargo" build --locked --release --target "$target" || build_status=$?
  fi
  if ((build_status == 0)); then
    install -m 755 "target/$target/release/pwget" "dist/pwget-$target"
  else
    failed+=("$target")
  fi
done

echo "Built binaries:"
if compgen -G 'dist/pwget-*' >/dev/null; then
  ls -lh dist/pwget-*
else
  echo "  none"
fi
if ((${#failed[@]})); then
  printf 'Build failures (install the target C toolchain/sysroot and retry):\n' >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi
