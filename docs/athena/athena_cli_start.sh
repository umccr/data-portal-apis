#!/usr/bin/env bash

# https://awscli.amazonaws.com/v2/documentation/api/latest/reference/athena/start-query-execution.html#examples

# Usage:
#   sh athena_cli_start.sh

AWS_REGION=ap-southeast-2
AWS_PROFILE=prodops
DATA_PORTAL=data_portal

# Paste your Athena SQL query between EOF i.e. bash heredoc syntax
read -rd '' sqlquery << EOF
select
    wfl.id,
    wfl.wfr_name,
    -- wfl.sample_name,
    wfl.type_name,
    wfl.wfr_id,
    wfl.wfl_id,
    wfl.wfv_id,
    wfl.version,
    -- wfl.input,
    wfl.start,
    -- wfl.output,
    "wfl"."end",
    wfl.end_status,
    wfl.notified,
    wfl.sequence_run_id,
    wfl.batch_run_id,
    wfl.portal_run_id
from
    data_portal.data_portal_workflow as wfl
where
    wfl.type_name in ('bcl_convert', 'BCL_CONVERT')
    -- and wfl.end_status in ('Succeeded', 'Failed', 'Aborted', 'Started', 'Deleted', 'Deleted;;issue475')
order by id desc;
EOF

# echo "$sqlquery"

aws athena start-query-execution \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query-string "$sqlquery" \
  --work-group "$DATA_PORTAL" \
  --query-execution-context Database="$DATA_PORTAL",Catalog="$DATA_PORTAL"
