# -*- coding: utf-8 -*-
# Use Case:
#   Using Portal API without needing PORTAL_TOKEN but, just natively with your existing AWS CLI credentials.
#
# Note:
#   Instead of going through Python library with `reticulate`, we could do the _pure_ R approach with
#   just `aws.signature` and `httr`.
#
# Usage:
#   conda activate data-portal-apis
#   pip install requests-aws4auth
#   export AWS_PROFILE=prodops
#   Rscript portal_api_sig4.R

depedencies <- list("reticulate", "aws.signature", "jsonlite", "testthat")
invisible(lapply(depedencies, function(d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(reticulate)
library(aws.signature)
library(jsonlite)
library(testthat)

print_dash <- function(times = 64) {
  cat(strrep("-", times))
  cat("\n")
}

use_condaenv(condaenv = "data-portal-apis")
py_config()

print_dash()

# ---

py_awsauth <- import("requests_aws4auth")
py_requests <- import("requests")
region <- "ap-southeast-2"
service <- "execute-api"

credentials <- aws.signature::locate_credentials()

authr <- py_awsauth$AWS4Auth(
  credentials$key,
  credentials$secret,
  region,
  service,
  session_token = credentials$session_token
)

url <- "https://api.data.prod.umccr.org/iam/lims"  # using iam endpoint

params <- reticulate::py_dict(
  c("rowsPerPage", "subject_id"),
  c("1000", "SBJ01651")
)

response <- py_requests$get(url, auth = authr, params = params)

response_list <- response$text
# response_list <- response$json()
# typeof(response_list)
# response_list

response_df <- fromJSON(response_list)

test_that("SBJ01651 should have only 3 samples/libraries", {
  expect_equal(response_df$pagination$count, 3)
})
print_dash()

results <- response_df$results
results

print_dash()

cat("All SBJ01651 samples:\n\t")
results$sample_id
results$sample_id[1]
results$sample_id[2]
results$sample_id[3]
