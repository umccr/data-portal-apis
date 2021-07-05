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
#   aws, docker, docker-compose
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

command -v docker-compose >/dev/null 2>&1 || {
  echo >&2 "DOCKER-COMPOSE COMMAND NOT FOUND. ABORTING..."
  exit 1
}

aws sts get-caller-identity >/dev/null 2>&1 || {
  echo >&2 "UNABLE TO LOCATE CREDENTIALS. YOUR AWS LOGIN SESSION HAVE EXPIRED. PLEASE LOGIN. ABORTING..."
  exit 1
}

# ---

echo "...Base project directory at $(pwd)"

db_container=$(docker-compose ps -q db)

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

load_localstack() {
  echo "...Loading mock data to localstack container"
  aws_local_cmd="aws --endpoint-url=http://localhost:4566"

  BUCKET_EXISTS=$(aws --endpoint-url=http://localhost:4566 s3api head-bucket --bucket test1 2>&1 || true)
  if [ -z "$BUCKET_EXISTS" ]; then
    true
  else
    eval "$aws_local_cmd s3 mb s3://test1"
  fi

  eval "$aws_local_cmd s3 cp ./README.md s3://test1"
  eval "$aws_local_cmd s3 ls"
  eval "$aws_local_cmd s3 ls s3://test1/"

  eval "$aws_local_cmd sqs create-queue --queue-name StdQueue"
  eval "$aws_local_cmd sqs create-queue --queue-name MyQueue.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true"

  sqs_report_event_queue_arn=$(aws ssm get-parameter --name '/data_portal/backend/sqs_report_event_queue_arn' --with-decryption | jq -r .Parameter.Value)
  sqs_report_event_queue_arn=${sqs_report_event_queue_arn##*:}
  eval "$aws_local_cmd sqs create-queue --queue-name $sqs_report_event_queue_arn"

  sqs_dragen_wgs_qc_queue_arn=$(aws ssm get-parameter --name '/data_portal/backend/sqs_dragen_wgs_qc_queue_arn' --with-decryption | jq -r .Parameter.Value)
  sqs_dragen_wgs_qc_queue_name=${sqs_dragen_wgs_qc_queue_arn##*:}
  eval "$aws_local_cmd sqs create-queue --queue-name $sqs_dragen_wgs_qc_queue_name --attributes FifoQueue=true,ContentBasedDeduplication=true"

  sqs_tumor_normal_queue_arn=$(aws ssm get-parameter --name '/data_portal/backend/sqs_tumor_normal_queue_arn' --with-decryption | jq -r .Parameter.Value)
  sqs_tumor_normal_queue_name=${sqs_tumor_normal_queue_arn##*:}
  eval "$aws_local_cmd sqs create-queue --queue-name $sqs_tumor_normal_queue_name --attributes FifoQueue=true,ContentBasedDeduplication=true"

  sqs_notification_queue_arn=$(aws ssm get-parameter --name '/data_portal/backend/sqs_notification_queue_arn' --with-decryption | jq -r .Parameter.Value)
  sqs_notification_queue_name=${sqs_notification_queue_arn##*:}
  eval "$aws_local_cmd sqs create-queue --queue-name $sqs_notification_queue_name --attributes FifoQueue=true,ContentBasedDeduplication=true"

  eval "$aws_local_cmd sqs list-queues"
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
