#!/usr/bin/env bash

## UMCCR Portal GDS PreSigned file download generator

## Portal Token
# See https://github.com/umccr/data-portal-apis/blob/dev/docs/PORTAL_TOKEN.md for how to setup
# PORTAL_TOKEN in your environment variable.
#
# For best practice, please try not code PORTAL_TOKEN=<XYZ> as variable here in
# the script to avoid accidentally secret leak.
# Whenever possible, please exercise temporary export into the current execution environment such as
#    export PORTAL_TOKEN=<SNIP>


## Prerequisite
# You should try your search keyword filter through Portal GDS search
# to get it right with the search keywords. You should do that
# - by visiting https://data.umccr.org
# - go to Subject of interest
# - go to Subject Data > GDS tab
# - at Search filter > entry your keywords e.g. `wts .bam$`
# - at search result table view, verify that results and total file count as expected
#
# Pro-tip:
# If doubt, try first with smaller counterpart file such as `.csv` or `.tsv` or `.txt`
# of the same meaningful magnitude; such that there exits `.bam` of similar from pipeline output.
#
# Note:
# When you make GET API request, typically the query parameter must be URL encoded.
# e.g. The following won't work:
# curl -s 'https://api.portal.prod.umccr.org/gds?search=wts .bam$'
#
# Encode URL on your request parameter string such as
# curl -s 'https://api.portal.prod.umccr.org/gds?search=wts%20.bam%24'
#
# There are multiple way to do URL encode. One quick way is as follows.
# Goto:   https://www.urlencoder.io
# Enter:  wts .bam$


##
# Usage:
#   export PORTAL_TOKEN=<SNIP>
#   sh gds_dl_gen.sh | tee gds_downloader.txt

##
# Config
#
ENDPOINT="https://api.portal.prod.umccr.org/gds"
#KEYWORDS="wts%20.bam%24"
KEYWORDS="wts%20fastqc_metrics%20.csv%24"
SUBJECT="SBJ00816"

##
# Step 1
# Extract GDS file records of interest by using Search filter keywords
#
curl -s -X GET -H "Authorization: Bearer $PORTAL_TOKEN" \
  -H "Content-Type: application/json" \
  "$ENDPOINT?rowsPerPage=1000&subject=$SUBJECT&search=$KEYWORDS" | jq > gds_records.json

##
# Step 2
# From above response, you can extract all ID fields and make PreSigned URL request
# https://github.com/umccr/data-portal-apis/blob/dev/docs/ENDPOINTS.md#gds-endpoint
#
# e.g
# The following jq gives you all ID:
#   jq -c '.results[] | .id' gds_records.json
#
# Then PreSigned request would be like:
#   curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/gds/425025/presign" | jq

# Then piping them all together would be like:
#jq -c '.results[] | .id' gds_records.json | xargs -I % curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "$ENDPOINT/%/presign" | jq -r '.signed_url' > gds_records__presigned.txt

# But. We shall do a tad more such that
# - using id to generate presigned url
# - then, using file name to pass-in curl output flag
jq -cr '.results[] | "\(.id) \(.name)"' gds_records.json | while read -r id name; do
    url=$(curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "$ENDPOINT/$id/presign" | jq -r '.signed_url')
    echo "curl -o '$name' '$url'" >> gds_downloader.txt
    #break
done

##
# Step 3
# If everything is good to this point, you can initiate start download process by simply invoke command
#   `sh gds_downloader.txt`
#
# You may normally do this in Gadi/Spartan HPC interactive session. If you would like to utilise multiple
# nodes for the task, you can further split the text file by lines as follows:
#   `split -l 8 gds_downloader.txt`
#
# Then you can request nodes to download them in parallel. Observe split files like so:
#   `less xaa`
#   `less xab`

##
# Finally, you should unset PORTAL_TOKEN such as
#   `env | grep PORTAL`
#   `unset PORTAL_TOKEN`
#   `env | grep PORTAL`
#
# And, clean up intermediate files
#   `rm -rf gds_records.json gds_downloader.txt xaa xab`
