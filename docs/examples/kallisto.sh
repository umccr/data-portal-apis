#!/usr/bin/env bash

# See https://github.com/umccr/data-portal-apis/blob/dev/docs/PORTAL_TOKEN.md for how to setup
# PORTAL_TOKEN in your environment variable.
#
# For best practice, please try not code PORTAL_TOKEN=<XYZ> as variable here in
# the script to avoid accidentally secret leak.
# Whenever possible, please exercise temporary export into the current execution environment such as
#    export PORTAL_TOKEN=<SNIP>

# NOTE: When you make GET API request, typically the query parameter must be URL encoded.
#
# e.g. The following won't work:
# curl -s "https://api.data.prod.umccr.org/s3?search=wts kallisto .tsv$"
# Encode URL on your request parameter string such as
# curl -s "https://api.data.prod.umccr.org/s3?search=wts%20kallisto%20.tsv%24"

# There are multiple way to do URL encode. One quick way is as follows.
# Goto:   https://www.urlencoder.io
# Enter:  wts kallisto .tsv$

##
# Usage:
#   export PORTAL_TOKEN=<SNIP>
#   sh kallisto.sh

KEYWORDS="wts%20kallisto%20.tsv%24"
SUBJECT="SBJ00816"

curl -s -X GET -H "Authorization: Bearer $PORTAL_TOKEN" \
  -H "Content-Type: application/json" \
  "https://api.data.prod.umccr.org/s3?rowsPerPage=1000&subject=$SUBJECT&search=$KEYWORDS" | jq > kallisto.json

##
# Next
# From above response, you can extract all ID fields and make PreSigned URL request
# https://github.com/umccr/data-portal-apis/blob/dev/docs/ENDPOINTS.md#s3-endpoint

# e.g
# The following jq gives you all ID:
#   jq -c '.results[] | .id' kallisto.json

# Then PreSigned request would be like:
#   curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3/425025/presign" | jq

# Then piping them all together would be like:
jq -c '.results[] | .id' kallisto.json | xargs -I % curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/s3/%/presign" | jq '.signed_url' > kallisto__presigned.json
