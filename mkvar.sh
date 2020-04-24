#!/usr/bin/env source
# -*- coding: utf-8 -*-
# Sourcing this script will perform
#   terraform workspace select <dev|prod>
#   terraform output
# and, all upper case terraform output variables will be exported into environment variables

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
    k=$(echo "$var" | cut -d'=' -f1)
    if [ -n "$2" ] && [ "$2" = "unset" ]; then
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
  echo "export STAGE=dev"
  export STAGE=dev
fi
