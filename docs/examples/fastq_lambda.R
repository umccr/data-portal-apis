# -*- coding: utf-8 -*-
#
# Note:
# This fastq_lambda work like `ls -l gds://vol/xxx` and aggreated by sample as in SampleSheet.
#
# Pseudocode:
# Traverse gds locations by using libica Python SDK.
# Aggreate BCL Convert output directory by sample as in SampleSheet.
#
# Usage:
#   aws sso login --profile prod && export AWS_PROFILE=prod && yawsso -p prod
#   Rscript fastq_lambda.R > out.txt

if (!require("aws.lambda")) install.packages("aws.lambda", repos = "https://cran.ms.unimelb.edu.au")

library("aws.lambda")

func_name <- "data-portal-api-prod-fastq"

gds_vol <- "gds://umccr-fastq-data-prod/"

# --

runs <- list(
  "210702_A00130_0165_BH7KFWDSX2",
  "210708_A00130_0166_AH7KTJDSX2"
)

locations <- lapply(runs, function (r) paste0(gds_vol, r))
# locations

results <- aws.lambda::invoke_function(
  func_name,
  region = "ap-southeast-2",
  payload = list(locations = locations)
)

# Note:
# Lambda invocation response "results" is in nested Python dict struct or a map (more generally); which is named list or
# pairlist in R. If we know the "key" in a map, accessing element is O(1). i.e. we have sample_library_name somewhere...
# Otherwise, it needs to iter over row-wise-ly. Contrast to column-major order.

# results
# names(results)
# names(results$fastq_map)  # all keys of the map

for (r in names(results$fastq_map)) {
  sample_library_name <- paste0(r)  # key of this_map
  print(sample_library_name)

  this_map <- results$fastq_map[[sample_library_name]]  # this_map has info on each sample entry as in SampleSheet
  # print(this_map)

  for (i in seq_along(this_map$fastq_list)) {  # we only interest in fastq_list of this_map
    read_i <- paste0("read_", i)
    print(read_i)

    read_i_fastq <- this_map$fastq_list[[i]]
    print(read_i_fastq)
  }
}

# Caveat:
# This form of accessing FASTQs is legacy usage as we are now in-favour of REST API endpoint.
# Please see
#   fastq_list_row.R
#   fastq_simple_list.R
