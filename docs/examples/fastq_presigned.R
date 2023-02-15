# -*- coding: utf-8 -*-
#
# Note:
# In this example, we will focus on FASTQs sharing/streaming/downloading use case with PreSigned URL from GDS.
# This may be useful for those who have Data Portal access through umccr.org identity. But no ICA account yet.
#
# Pseudocode:
#   1) we call fastq endpoint for each Runs, combining each JSON response into 1 single dataframe, results_df
#   2) from results_df, we keep read_1 and read_2 columns (FastqListRow model) and create location list for POST payload
#   3) then we call presign endpoint with list of gds locations as payload body
#   4) parse presign response JSON into dataframe
#
#   presigned_url are signed by service user
#
# Usage:
#   export PORTAL_TOKEN=eyxxX<...>
#   Rscript fastq_presigned.R > out.txt

depedencies <- list("httr", "jsonlite")
invisible(lapply(depedencies, function (d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(httr)
library(jsonlite)

print_dash <- function (times = 64) {
  cat(strrep("-", times))
  cat("\n")
}

# --

base_url <- "https://api.portal.prod.umccr.org/"

endpoint_fastq <- "fastq"
endpoint_presign <- "presign"

portal_token <- Sys.getenv("PORTAL_TOKEN")

H <- add_headers(Authorization = paste0("Bearer ", portal_token))

# --

runs <- list(
  "210624_A01052_0052_AH7KFMDSX2",
  "210820_A01052_0058_AHGJM3DSX2",
  "211014_A00130_0179_AHLFYJDSX2",
  "211014_A00130_0180_BHLGF7DSX2"
)

project_owner <- "Tothill"  # optionally filter project_owner

# --

responses <- lapply(runs, function (run) GET(base_url, path = endpoint_fastq, query = list(
  rowsPerPage = 1000,
  run = run,
  project_owner = project_owner  # comment this if you do not want to filter
), H))
# responses

results_df <- data.frame()
for (resp in responses) {
  resp_body <- content(resp, as = "text", encoding = "UTF-8")
  df <- fromJSON(resp_body)
  results_df <- rbind(results_df, df$results)
}
# results_df

cols <- c("read_1", "read_2")
payload_locations_vector <- as.vector(t(results_df[cols]))
# payload_locations_vector
payload_locations <- as.list(payload_locations_vector)
# payload_locations

# --
# print_dash()

resp_presign <- POST(base_url, path = endpoint_presign, body = payload_locations, encode = "json", H)
resp_presign_body <- content(resp_presign, as = "text", encoding = "UTF-8")
presign_body_df <- fromJSON(resp_presign_body)
# presign_body_df

signed_urls_df <- presign_body_df$signed_urls
# signed_urls_df
# signed_urls_df$presigned_url

# Just printing row-isely for example
for (row in seq_len(nrow(signed_urls_df))) {
  volume <- signed_urls_df[row, "volume"]
  path <- signed_urls_df[row, "path"]
  presigned_url <- signed_urls_df[row, "presigned_url"]
  print(paste0("gds://", volume, path))
  print(presigned_url)
  print_dash()
}
