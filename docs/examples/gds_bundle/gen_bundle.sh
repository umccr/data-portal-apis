#!/usr/bin/env bash

###
# Usage:
#   bash gen_gds_bundle.sh tothill_cup
#

# ---

_deps() {
  cmds="ica jq tee"
  for i in $cmds; do
    if command -v "$i" >/dev/null; then
      continue
    else
      echo "Command '$i' is required"
      exit 1
    fi
  done
}
_deps

# ---

if [ $# -eq 0 ]; then
  echo >&2 "Usage: bash <script> <my_bundle_prefix>"
  exit 1
fi

bundle_prefix="${1}"

folders="gds_folders.txt"

echo "Making bundle..."

if [[ -f "${bundle_prefix}_bundle.ndjson" ]]; then
  echo "Existing ${bundle_prefix}_bundle.ndjson found! Please remove it first."
  exit 0
fi

while IFS= read -r folder; do
  echo "${folder}"
  ica files list ${folder} --max-items 0 --page-size 10000 --with-access -o json |
    jq -c '[.items[] | {path: .path, presignedUrl: .presignedUrl}]' |
    tee -a ${bundle_prefix}_bundle.ndjson >/dev/null
done <${folders}

echo "Created bundle: ${bundle_prefix}_bundle.ndjson"
