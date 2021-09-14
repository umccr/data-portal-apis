# -*- coding: utf-8 -*-
#
# In this example, we will use pairing endpoint for Tumor / Normal FASTQ pairs for given Runs.
# Please note we are to POST request to pairing endpoint because we request endpoint to create a T/N pairing list.
# The outer dataframe has these columns:
#  | subject_id | fastq_list_rows | tumor_fastq_list_rows | output_file_prefix | output_directory | sample_name |
#
# Just like fastq endpoint, pairing endpoint return FASTQs in a form of Portal's FastqListRow model.
# This FastqListRow model has these columns:
#  | rgid | rgsm | rglb | lane | read_1 | read_2 |
#
# 'rgid' is unique identifier for this record
# 'rgsm' is sample identifier
# 'rglb' is library identifier
# 'lane' is lane number
# 'read_1' is an absolute path to FASTQ location on GDS
# 'read_2' is an absolute path to FASTQ location on GDS
#
# Pseudocode:
# Call each run
#   curl -s -X POST -d '["210903_A00130_0170_AHGKJ7DSX2", "210909_A00130_0171_BHGKN7DSX2"]' -H "Content-Type: application/json" -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.data.prod.umccr.org/pairing"
#
# Then we will parse response text into JSON into dataframe. FastqListRow is in nested dataframe.
#
# Usage:
#   export PORTAL_TOKEN=eyxxX<...>
#   Rscript pairing_tn_fastq.R > out.txt

depedencies <- list("httr", "jsonlite")
invisible(lapply(depedencies, function (d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(httr)
library(jsonlite)

print_dash <- function (times = 64) {
  cat(strrep("-", times))
  cat("\n")
}

# --

base_url <- "https://api.data.prod.umccr.org/"

endpoint <- "pairing"

portal_token <- Sys.getenv("PORTAL_TOKEN")

H <- add_headers(Authorization = paste0("Bearer ", portal_token))

# --

runs <- list(
  "210903_A00130_0170_AHGKJ7DSX2",
  "210909_A00130_0171_BHGKN7DSX2"
)

resp <- POST(base_url, path = endpoint, body = runs, encode = "json", H)
# http_type(resp)
# status_code(resp)
# str(content(resp))  # response body content to string

resp_body <- content(resp, as = "text", encoding = "UTF-8")
df <- fromJSON(resp_body)
df

print_dash()

# let pick the first vector
idx <- 1

subjects <- df$subject_id
subjects[[idx]]

tumor_samples <- df$sample_name
tumor_samples[[idx]]

normal_fqlr_df <- df$fastq_list_rows
normal_fqlr_df[[idx]]

tumor_fqlr_df <- df$tumor_fastq_list_rows
tumor_fqlr_df[[idx]]

print_dash()

# Next:
# Similarly, instead of by_sequnce_runs, you can also use by_subjects, by_libraries, by_samples
# Please modify endpoint accordingly

endpoint <- "pairing/by_subjects"
subjects <- list("SBJ00989", "SBJ00990", "SBJ00991", "SBJ00992")
resp <- POST(base_url, path = endpoint, body = subjects, encode = "json", H)
resp_body <- content(resp, as = "text", encoding = "UTF-8")
df <- fromJSON(resp_body)
df
