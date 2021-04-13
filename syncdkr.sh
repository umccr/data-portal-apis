#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# This script pull the said docker image:tag from docker hub and push into DEV account ECR.
# If you updated image version, make sure to run sync docker image once to DEV ECR repo.
# Usage:
#   sh syncdkr.sh

ECR_DEV=843407916570.dkr.ecr.ap-southeast-2.amazonaws.com

MYSQL=mysql:5.7
HAPROXY=haproxy:2.3
PRISM_3=stoplight/prism:3
PRISM_4=stoplight/prism:4
LOCALSTACK=localstack/localstack:0.12.9

declare -a images=("$MYSQL" "$HAPROXY" "$PRISM_3" "$PRISM_4" "$LOCALSTACK")

command -v aws >/dev/null 2>&1 || {
  echo >&2 "AWS CLI COMMAND NOT FOUND. ABORTING..."
  exit 1
}

command -v docker >/dev/null 2>&1 || {
  echo >&2 "DOCKER COMMAND NOT FOUND. ABORTING..."
  exit 1
}

aws sts get-caller-identity >/dev/null 2>&1 || {
  echo >&2 "UNABLE TO LOCATE CREDENTIALS. YOUR AWS LOGIN SESSION HAVE EXPIRED. PLEASE LOGIN. ABORTING..."
  exit 1
}

aws ecr get-login-password | docker login --username AWS --password-stdin $ECR_DEV

for i in "${images[@]}"; do
  docker pull "$i"
  docker image tag "$i" $ECR_DEV/"$i"
  docker push $ECR_DEV/"$i"
done
