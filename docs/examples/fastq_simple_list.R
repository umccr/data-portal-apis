# -*- coding: utf-8 -*-
#
# Note:
# Portal also capture ENS GDS File events and index the GDS File metadata that drops into a particular
# monitored GDS volumes. Hence, you can also search FASTQs from this index database; this offers through
# endpoint named "gds". This "gds" endpoint is the backing of Portal UI on browsable GDS tab.
#
# Pseudocode:
#   curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/gds?rowsPerPage=1000&run=210722_A01052_0056_AHGJT7DSX2&search=.fastq.gz$" | jq
#
# Usage:
#   export PORTAL_TOKEN=eyxxX<...>
#   Rscript fastq_simple_list.R > out.txt

depedencies <- list("httr", "jsonlite")
invisible(lapply(depedencies, function (d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(httr)
library(jsonlite)

# --

base_url <- "https://api.data.prod.umccr.org/"

endpoint <- "gds"

portal_token <- Sys.getenv("PORTAL_TOKEN")

H <- add_headers(Authorization = paste0("Bearer ", portal_token))

# --

run <- "210722_A01052_0056_AHGJT7DSX2"

resp <- GET(base_url, path = endpoint, query = list(rowsPerPage = 1000, run = run, search=".fastq.gz$"), H)
# http_type(resp)
# status_code(resp)
# str(content(resp))  # response body content to string

resp_body <- content(resp, as = "text", encoding = "UTF-8")
df <- fromJSON(resp_body)
# df
df$results

# Typically columns "name", "volume_name", "path" are what you might be after.
# You can build absolute gds path by paste0("gds://", volume_name, path)

# Columns "file_id" and "volume_id" may be useful with interfacing ICA GDS directly;
# such as requesting Presigned URL. e.g.  ica files get <file_id> -o json | jq '.presignedUrl'
# They are unique identifier for ICA GDS File and Volume record.

# Column "id" is Portal internal unique identifier for that record. And "unique_hash" column is
# sha256 checksum of (volume_name, path) which use as unique key constraint checker for indexing.
