#!/usr/bin/env bash
# Copyright (c) 2026 Lark Technologies Pte. Ltd.
# SPDX-License-Identifier: MIT

set -euo pipefail

workflow=".github/workflows/release.yml"
goreleaser=".goreleaser.yml"
verifier="scripts/verify-release-assets.sh"

release_config="$(
  awk '
    /^release:/ { in_release = 1; print; next }
    in_release && /^[^[:space:]]/ { exit }
    in_release { print }
  ' "$goreleaser"
)"
if ! grep -Eq '^  draft: true$' <<<"$release_config"; then
  echo "GoReleaser must keep the GitHub release as a draft until verification completes" >&2
  exit 1
fi
if ! grep -Eq '^  replace_existing_draft: true$' <<<"$release_config"; then
  echo "GoReleaser should make failed draft releases safely retryable" >&2
  exit 1
fi

step_line() {
  local name="$1"
  local matches
  matches="$(grep -nF -- "- name: $name" "$workflow" || true)"
  if [[ "$(wc -l <<<"$matches" | tr -d ' ')" != "1" || -z "$matches" ]]; then
    echo "release workflow must contain exactly one '$name' step" >&2
    exit 1
  fi
  cut -d: -f1 <<<"$matches"
}

build_line="$(step_line "Build and upload draft release with GoReleaser")"
checksum_line="$(step_line "Include release checksums")"
identity_line="$(step_line "Verify release edition identities")"
platform_line="$(step_line "Verify release platform asset matrix")"
attest_line="$(step_line "Attest release archives")"
collect_line="$(step_line "Collect release asset")"
artifact_line="$(step_line "Upload release asset")"
publish_line="$(step_line "Publish verified GitHub release")"
publish_npm_line="$(grep -n '^  publish-npm:' "$workflow" | cut -d: -f1)"

previous=0
for line in \
  "$build_line" \
  "$checksum_line" \
  "$identity_line" \
  "$platform_line" \
  "$attest_line" \
  "$collect_line" \
  "$artifact_line" \
  "$publish_line" \
  "$publish_npm_line"; do
  if (( line <= previous )); then
    echo "release must build a draft, verify and attest it, then publish it as the final build-release step" >&2
    exit 1
  fi
  previous="$line"
done

attest_step="$(sed -n "${attest_line},$((collect_line - 1))p" "$workflow")"
if ! grep -Fq 'actions/attest-build-provenance@e8998f949152b193b063cb0ec769d69d929409be' <<<"$attest_step"; then
  echo "release provenance must use the pinned attest-build-provenance action" >&2
  exit 1
fi
for subject in \
  '            dist/*.tar.gz' \
  '            dist/*.zip' \
  '            dist/checksums.txt' \
  '            dist/install-extended.sh' \
  '            dist/install-extended.ps1'; do
  if [[ "$(grep -Fxc "$subject" <<<"$attest_step")" != "1" ]]; then
    echo "release provenance is missing exact subject: ${subject#            }" >&2
    exit 1
  fi
done

build_release_tail="$(sed -n "${publish_line},$((publish_npm_line - 1))p" "$workflow")"
if [[ "$(grep -Ec '^      - (name|uses):' <<<"$build_release_tail")" != "1" ]]; then
  echo "no build-release step may run after the verified GitHub release becomes public" >&2
  exit 1
fi

for required in \
  'bash scripts/verify-release-assets.sh dist "${GITHUB_REF_NAME#v}"' \
  'dist/checksums.txt' \
  'if (!release.draft)' \
  'asset.digest.toLowerCase() !== expected' \
  'draft: false' \
  'make_latest: "true"'; do
  if ! grep -Fq "$required" "$workflow"; then
    echo "release workflow is missing publication contract: $required" >&2
    exit 1
  fi
done

if ! grep -Fq 'actions/github-script@ed597411d8f924073f98dfc5c65a23a2325f34cd' "$workflow"; then
  echo "verified release publication must use the pinned github-script action" >&2
  exit 1
fi

bash -n "$verifier"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/lark-cli-release-contract.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT
dist_dir="$tmp_dir/dist"
unix_payload="$tmp_dir/unix"
windows_payload="$tmp_dir/windows"
mkdir -p "$dist_dir" "$unix_payload" "$windows_payload"

for payload in "$unix_payload" "$windows_payload"; do
  printf 'changelog\n' >"$payload/CHANGELOG.md"
  printf 'license\n' >"$payload/LICENSE"
  printf 'readme\n' >"$payload/README.md"
done
printf '#!/bin/sh\nexit 0\n' >"$unix_payload/lark-cli"
printf 'synthetic windows binary\n' >"$windows_payload/lark-cli.exe"

version="1.2.3"
platforms=(
  darwin-amd64.tar.gz
  darwin-arm64.tar.gz
  linux-amd64.tar.gz
  linux-arm64.tar.gz
  linux-riscv64.tar.gz
  windows-amd64.zip
  windows-arm64.zip
)
for platform in "${platforms[@]}"; do
  for prefix in lark-cli lark-cli-extended; do
    archive="$dist_dir/${prefix}-${version}-${platform}"
    if [[ "$platform" == *.zip ]]; then
      (
        cd "$windows_payload"
        zip -q "$archive" CHANGELOG.md LICENSE README.md lark-cli.exe
      )
    else
      tar -czf "$archive" -C "$unix_payload" CHANGELOG.md LICENSE README.md lark-cli
    fi
  done
done
printf '#!/bin/sh\n' >"$dist_dir/install-extended.sh"
printf 'Write-Host "install"\n' >"$dist_dir/install-extended.ps1"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi
  shasum -a 256 "$1" | awk '{print $1}'
}

for asset_path in "$dist_dir"/lark-cli-*.tar.gz "$dist_dir"/lark-cli-*.zip \
  "$dist_dir/install-extended.sh" "$dist_dir/install-extended.ps1"; do
  printf '%s  %s\n' "$(sha256_file "$asset_path")" "$(basename "$asset_path")"
done | LC_ALL=C sort -k2 >"$dist_dir/checksums.txt"

bash "$verifier" "$dist_dir" "$version" >/dev/null
printf 'tamper\n' >>"$dist_dir/lark-cli-${version}-linux-amd64.tar.gz"
if bash "$verifier" "$dist_dir" "$version" >/dev/null 2>&1; then
  echo "release asset verifier must reject a checksum mismatch" >&2
  exit 1
fi
