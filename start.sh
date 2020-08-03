#!/usr/bin/env source
# -*- coding: utf-8 -*-
# This wrapper script is mainly for Serverless and local development purpose only.
#
# Sourcing this script will perform:
#   1. get required config values from SSM Parameter Store
#   2. export them as environment variables
#
# REQUIRED CLI:
#   aws, jq, sed, awk, cut, tr
#
# USAGE:
#   source start.sh
#   source start.sh check
#   source start.sh unset
#
# CAVEATS:
# By default it sources all SSM parameters by path under /data_portal/backend/
#
# Try to be POSIX-y. Only tested on macOS! Contrib welcome for other OSs.

params_path=${SSM_PARAMETERS_PATH:-/data_portal/backend/}

# ---

if [ "$(ps -p $$ -ocomm=)" = 'zsh' ] || [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  ps -p $$ -oargs=
  echo "YOU SHOULD SOURCE THIS SCRIPT, NOT EXECUTE IT!"
  exit 1
fi

command -v aws >/dev/null 2>&1 || {
  echo >&2 "AWS CLI COMMAND NOT FOUND. ABORTING..."
  return 1
}

command -v jq >/dev/null 2>&1 || {
  echo >&2 "JQ COMMAND NOT FOUND. ABORTING..."
  return 1
}

params_json=$(aws ssm get-parameters-by-path --path "$params_path" --with-decryption)
if [[ "$params_json" == "" ]]; then
  echo "Halt, No valid AWS login session found. Please 'aws sso login --profile dev && export AWS_PROFILE=dev'"
  return 1
fi

params=$(echo "$params_json" | jq '.Parameters[] | [.Name, .Value] | @tsv')

while read -r line; do
  l=$(sed -e 's/^"//' -e 's/"$//' <<<"$line")
  name=$(echo "$l" | awk '{print $1}' | cut -d '/' -f4 | tr '[:lower:]' '[:upper:]')
  value=$(echo "$l" | awk '{print $2}')
  if [ -n "$1" ] && [ "$1" = "unset" ]; then
    echo "unset $name"
    unset "$name"
  elif [ -n "$1" ] && [ "$1" = "check" ]; then
    env | grep "$name"
  else
    echo "export $name=$value"
    export "$name=$value"
  fi
done <<EOF
$params
EOF
