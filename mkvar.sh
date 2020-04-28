#!/usr/bin/env source
# -*- coding: utf-8 -*-
# Sourcing this script will perform
#   1. terraform workspace select <dev|prod>
#   2. terraform output
#   3. and, all upper case terraform output variables will be exported into environment variables
#
# CAVEATS:
# Tried to be POSIX-y shell script with cut, grep, tr for text wrangling.
# Tested on macOS. Should ever fail this, try export env var from terraform output as steps above.
# Only needed for Serverless purpose though, see ${env:XXX} in serverless.yml.
# Local development setup does not depends on these AWS specific env var.

if [ "$(ps -p $$ -ocomm=)" = 'zsh' ] || [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    ps -p $$ -oargs=
    echo "YOU SHOULD SOURCE THIS SCRIPT, NOT EXECUTE IT!"
    exit 1
fi

command -v terraform >/dev/null 2>&1 || {
  echo >&2 "TERRAFORM COMMAND NOT FOUND. ABORTING..."
  return 1
}

[ -z "$1" ] && {
  echo "NO ARGUMENT SUPPLIED. USAGE: source mkvar.sh dev (OR) source mkvar.sh dev unset"
  return 1
}

if [ "$1" != "dev" ] && [ "$1" != "prod" ]; then
  echo "PROVIDE DEV OR PROD. USAGE: source mkvar.sh dev (OR) source mkvar.sh dev unset"
  return 1
fi

terraform workspace select "$1"
out=$(terraform output)

while IFS= read -r line; do
  l=$(echo "$line" | cut -c1-2)
  if echo "$l" | grep -Eq '[A-Z]'; then
    var=$(echo "$line" | tr -d " ")
    if [ -n "$2" ] && [ "$2" = "unset" ]; then
      k=$(echo "$var" | cut -d'=' -f1)
      echo "unset $k"
      unset "$k"
    else
      echo "export $var"
      export "${var?}"
    fi
  fi
done <<EOF
$out
EOF

if [ -n "$2" ] && [ "$2" = "unset" ]; then
  echo "unset STAGE"
  unset STAGE
else
  echo "export STAGE=$1"
  export STAGE=$1
fi

terraform workspace select default
