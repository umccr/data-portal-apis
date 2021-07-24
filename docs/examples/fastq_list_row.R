# -*- coding: utf-8 -*-
#
# Note:
# This fastq endpoint return FASTQs in a form of Portal's FastqListRow model. This model has these columns:
#  | rgid | rgsm | rglb | lane | read_1 | read_2 | sequence_run |
#
# 'id' is Portal internal unique identifier to this record
# 'rgid' is unique identifier for this record
# 'rgsm' is sample identifier
# 'rglb' is library identifier
# 'lane' is lane number
# 'read_1' is an absolute path to FASTQ location on GDS
# 'read_2' is an absolute path to FASTQ location on GDS
# 'sequence_run' is Portal internal unique identifier to this SequenceRun (can be used with /sequence/<ID> endpoint)
#
# Pseudocode:
# Call each run
#   curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?rowsPerPage=1000&run=210702_A00130_0165_BH7KFWDSX2" | jq
#   curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/fastq?rowsPerPage=1000&run=210708_A00130_0166_AH7KTJDSX2" | jq
# And append the response into the list.
# Then iterate the response list, parse JSON body content, extract results into dataframe and combine them.
#
# Usage:
#   export PORTAL_TOKEN=eyxxX<...>
#   Rscript fastq_list_row.R > out.txt

depedencies <- list("httr", "jsonlite")
invisible(lapply(depedencies, function (d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(httr)
library(jsonlite)

# --

base_url <- "https://api.data.prod.umccr.org/"

endpoint <- "fastq"

portal_token <- Sys.getenv("PORTAL_TOKEN")

H <- add_headers(Authorization = paste0("Bearer ", portal_token))

# --

runs <- list(
  "210702_A00130_0165_BH7KFWDSX2",
  "210708_A00130_0166_AH7KTJDSX2"
)

responses <- lapply(runs, function (run) GET(base_url, path = endpoint, query = list(rowsPerPage = 1000, run = run), H))
# responses

results_df <- data.frame()
for (resp in responses) {
  # http_type(resp)
  # status_code(resp)
  # str(content(resp))  # response body content to string

  resp_body <- content(resp, as = "text", encoding = "UTF-8")
  df <- fromJSON(resp_body)
  results_df <- rbind(results_df, df$results)
}
results_df
