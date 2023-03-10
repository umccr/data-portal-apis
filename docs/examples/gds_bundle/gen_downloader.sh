#!/usr/bin/env bash

###
# Usage:
#   bash gen_downloader.sh tothill_cup_bundle.ndjson
#

# ---

_deps() {
  cmds="jq tee"
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
  echo >&2 "Usage: bash <script> <my_bundle.ndjson>"
  exit 1
fi

filename=$(basename -- "${1}")
extension="${filename##*.}"
filename="${filename%.*}"

if [[ -f "${filename}.downloader" ]]; then
  echo "Existing ${filename}.downloader found! Please remove it first."
  exit 0
fi

while IFS= read -r line; do
  jq -cr '.[] | "\(.path) \(.presignedUrl)"' <<<${line} | while read -r path url; do

    # exclude files by extension, comment these lines if no use
#    name=$(basename -- "${path}")
#    ext="${name##*.}"
#    if [ "${ext}" = "bam" ]; then
#      echo "Skipping file: ${path}"
#      continue
#    fi

    echo "echo '${path}' && curl --progress-bar --create-dirs -C - -o '.${path}' '${url}'" | tee -a ${filename}.downloader >/dev/null

    # uncomment this break, if you'd just like try first line only
#    break

  done
done <${1}
