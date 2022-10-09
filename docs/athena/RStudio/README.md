# Portal Athena with RStudio

This shows how to use _RStudio IDE as R/SQL Client_ with Portal AWS Athena setup. 

This uses [R-DBI library](https://dbi.r-dbi.org) through ODBC connection to Athena. If this does not work, please see [CLI](../README_CLI.md) or [Programmatic section](../README.md) for alternate.


## Setup

### Step 1

- Download and install "**Simba Athena ODBC Driver**" from AWS[1]

### Step 2

- Login UMCCR AWS Prod account as `ProdOperator` as usual

```
aws sso login --profile prodops
```

### Step 3

- Create R script file, e.g. `athena.R`
- As in [2], construct `con` DBI connection object in regard to UMCCR AWS profile.

```
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver             = "Simba Athena ODBC Driver",
  AwsRegion          = "ap-southeast-2",
  AuthenticationType = "IAM Profile",
  Workgroup          = "data_portal",
  AWSProfile         = "prodops"
)
```

### Step 4

- Create SQL script file, e.g. `athena.sql`
- Use SQL comment macro in `.sql` file with reference to `con` connection object created from step 2. RStudio will render result table in preview panel. e.g.

```sql
-- !preview conn=con

select count(1) from "AwsDataCatalog"."multiqc"."dragen_umccrise";
```

- You can also use more programmatic way with R through `glue` or `dbplyr` then `ggplot2`. See  [3] for more details.


## Starter Demo

- [athena.R](athena.R)
- [athena.sql](athena.sql)
- [athena_federated_query.sql](athena_federated_query.sql)

![rstudio_dbi_odbc_athena_1.png](rstudio_dbi_odbc_athena_1.png)

![rstudio_dbi_odbc_athena_2.png](rstudio_dbi_odbc_athena_2.png)

![rstudio_dbi_odbc_athena_federated_query.png](rstudio_dbi_odbc_athena_federated_query.png)


## REF

* [1] https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html
* [2] https://solutions.rstudio.com/db/databases/athena/
* [3] https://www.rstudio.com/blog/working-with-databases-and-sql-in-rstudio/
