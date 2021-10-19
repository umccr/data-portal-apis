# -*- coding: utf-8 -*-
#
# Note:
# This fastq_lambda work like `ls -l gds://vol/xxx` and aggreated by sample as in SampleSheet.
#
# Caveat:
# This form of accessing FASTQs is legacy usage as we are now in-favour of REST API endpoint.
# Please see
#   fastq_list_row.R
#   fastq_simple_list.R
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

# --

print_dash <- function (times = 64) {
  cat(strrep("-", times))
  cat("\n")
}

func_name <- "data-portal-api-prod-fastq"

# --

# Please note that recent FASTQ output are harmonised to new base location gds://production/primary_data/ i.e.
# start from Run 168 onwards. Some older runs are still in previous base location gds://umccr-fastq-data-prod/
# You may provide either base location as fastq Lambda works on absolute location at any case.

locations <- list(
  "gds://umccr-fastq-data-prod/210708_A00130_0166_AH7KTJDSX2/",
  "gds://production/primary_data/210830_A00130_0168_AHGKVWDSX2/"
)
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
    # read_i <- paste0("read_", i)
    # print(read_i)

    read_i_fastq <- this_map$fastq_list[[i]]
    print(read_i_fastq)
  }
}

# --
print_dash()

locations <- list(
  "gds://production/primary_data/211014_A00130_0180_BHLGF7DSX2/"
)

project_owner <- list(
  "Bedoui"
)

# It is possible to filter by project_owner.
results <- aws.lambda::invoke_function(
  func_name,
  region = "ap-southeast-2",
  payload = list(
    locations = locations,
    project_owner = project_owner
  )
)
# results

for (r in names(results$fastq_map)) {
  sample_library_name <- paste0(r)
  print(sprintf("sample: %s", sample_library_name))

  this_map <- results$fastq_map[[sample_library_name]]

  for (i in seq_along(this_map$fastq_list)) {
    read_i <- paste0("read_", i)
    read_i_fastq <- this_map$fastq_list[[i]]
    print(sprintf("%s: %s", read_i, read_i_fastq))
  }

  # We can iterate tags
  # for (i in seq_along(this_map$tags)) {
  #   tag_i <- paste0("tag_", i)
  #   print(tag_i)
  #
  #   tag_i_element <- this_map$tags[[i]]
  #   print(tag_i_element)
  # }
  # However there are only be 1 tag avail at the mo and, has these properties
  this_tag <- this_map$tags[[1]]
  print(sprintf("subject_id: %s", this_tag$subject_id))
  print(sprintf("project_owner: %s", this_tag$project_owner))
  print(sprintf("project_name: %s", this_tag$project_name))
}

# --
print_dash()

# You may also just get the flat list instead!!
results <- aws.lambda::invoke_function(
  func_name,
  region = "ap-southeast-2",
  payload = list(
    locations = locations,
    project_owner = list("Tothill"),
    flat = TRUE
  )
)
results
