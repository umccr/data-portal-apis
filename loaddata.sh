#!/usr/bin/env sh
# -*- coding: utf-8 -*-
# This script is mainly for local development purpose only.
#
# Running this script can perform:
#   1. sync db dump from S3 bucket
#   2. copy db dump into local stack db container
#   3. load db dump into local stack db container
#
# REQUIRED CLI:
#   aws, docker
#
# USAGE:
# Typically use in conjunction with Makefile:
#   make syncdata
#   make loaddata
#
# Otherwise run it standalone itself by:
#   sh loaddata.sh sync
#   sh loaddata.sh copy
#   sh loaddata.sh load
#
# CAVEATS:
# By default it sync db dump from s3://umccr-data-portal-build-dev/data_portal.sql.gz
#
# Try to be POSIX-y. Only tested on macOS! Contrib welcome for other OSs.

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

# ---

echo "...Base project directory at $(pwd)"

db_container="portal_db"

sync_db_dump() {
  echo "...Syncing database dump from S3 bucket"
  mkdir -p data
  aws s3 sync s3://umccr-data-portal-build-dev/ data/ --exclude='*' --include='data_portal.sql.gz'
}

copy_db_dump() {
  echo "...Copying database dump to db container"
  docker cp data/data_portal.sql.gz "$db_container":/
}

load_db_dump() {
  echo "...Loading database dump to db container"
  docker exec -i -e MYSQL_PWD=data_portal "$db_container" \
    mysql -udata_portal -e"DROP DATABASE IF EXISTS data_portal;CREATE DATABASE IF NOT EXISTS data_portal;"
  docker exec -i -e MYSQL_PWD=data_portal "$db_container" \
    /bin/bash -c 'zcat data_portal.sql.gz | mysql -udata_portal data_portal'
}

## SQS Queue helper functions
get_sqs_queue_from_ssm_parameter_name(){
  : '
  Get the sqs queue arn from the input ssm parameter
  '
  aws ssm get-parameter \
      --output json \
      --name "$1" \
      --with-decryption | \
  jq --raw-output '.Parameter.Value'
}

get_sqs_queue_name_from_arn(){
  : '
  From:
    "arn:aws:sqs:ap-southeast-2:843407916570:data-portal-dragen-wgs-qc-queue.fifo"
  To:
    "data-portal-dragen-wgs-qc-queue.fifo"
  '
  echo "${1##*:}"
}

create_standard_sqs_queue(){
  : '
  Create a standard queue based on the queue name
  '
  eval \
    '${aws_local_cmd} sqs create-queue' \
    '--output json' \
    '--queue-name "$1"'
}

create_fifo_sqs_queue(){
  : '
  Create a fifo queue based on the queue name
  '
  eval \
    '${aws_local_cmd} sqs create-queue' \
    '--output json' \
    '--queue-name "$1"' \
    '--attributes "FifoQueue=true,ContentBasedDeduplication=true"'
}

create_fifo_sqs_queue_from_ssm_parameter(){
  : '
  Create the sqs queue with the name based on the arn value of the ssm parameter
  '
  _sqs_arn_value="$( \
    get_sqs_queue_from_ssm_parameter_name "${1}" \
  )"

  _sqs_queue_name="$( \
    get_sqs_queue_name_from_arn "${_sqs_arn_value}"
  )"

  create_fifo_sqs_queue "${_sqs_queue_name}"
}

load_localstack() {
  echo "...Loading mock data to localstack container"
  aws_local_cmd="aws --endpoint-url=http://localhost:4566"

  BUCKET_EXISTS=$(aws --endpoint-url=http://localhost:4566 s3api head-bucket --bucket test1 2>&1 || true)
  if [ -z "$BUCKET_EXISTS" ]; then
    true
  else
    eval "${aws_local_cmd} s3 mb s3://test1"
  fi

  eval "${aws_local_cmd} s3 cp ./README.md s3://test1"
  eval "${aws_local_cmd} s3 ls"
  eval "${aws_local_cmd} s3 ls s3://test1/"

  create_standard_sqs_queue "StdQueue"
  create_fifo_sqs_queue "MyQueue.fifo"

  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_dragen_wgs_qc_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_dragen_wts_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_dragen_tso_ctdna_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_tumor_normal_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_umccrise_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_rnasum_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_somalier_extract_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_notification_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_star_alignment_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_oncoanalyser_wts_queue_arn'
  create_fifo_sqs_queue_from_ssm_parameter '/data_portal/backend/sqs_oncoanalyser_wgs_queue_arn'

  eval '${aws_local_cmd} sqs list-queues --output json'
}

if [ -n "$1" ] && [ "$1" = "sync" ]; then
  sync_db_dump
elif [ -n "$1" ] && [ "$1" = "copy" ]; then
  copy_db_dump
elif [ -n "$1" ] && [ "$1" = "load" ]; then
  load_db_dump
else
  echo "...Available options:  sync, copy, load"
fi
