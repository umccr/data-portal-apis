#install.packages("DBI", repos = "https://cran.ms.unimelb.edu.au")
#install.packages("rstudioapi", repos = "https://cran.ms.unimelb.edu.au")
#install.packages("odbc", repos = "https://cran.ms.unimelb.edu.au")
#install.packages("glue", repos = "https://cran.ms.unimelb.edu.au")

library(DBI)
library(rstudioapi)
library(odbc)
library(glue)

# Option 1: Using "User DSN"
#con <- DBI::dbConnect(odbc::odbc(), "PortalAthena")

# Option 2: Using IAM Profile
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver             = "Simba Athena ODBC Driver",
  AwsRegion          = "ap-southeast-2",
  AuthenticationType = "IAM Profile",
  Workgroup          = "data_portal",
  AWSProfile         = "prodops"
)

tbl_glue <-
  glue::glue_sql('
    select 
        min(cov_avg_seq_over_genome_dragen) as min_cov_avg_seq_over_genome_dragen, 
        max(cov_avg_seq_over_genome_dragen) as max_cov_avg_seq_over_genome_dragen
    from 
        "AwsDataCatalog"."multiqc"."dragen_umccrise"
  ')

DBI::dbGetQuery(con, tbl_glue)
