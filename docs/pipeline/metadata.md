# Portal Metadata

Portal has data processing Lambdas for scheduled or on-demand sync with "upstream" metadata sources as "source-of-truth". 

> Design Note:
> 
> While Portal offers a convenience way to access these metadata through `/metadata` and `/lims` endpoints; or through Portal Athena; it is to note that "ownership" of these metadata and its structures are all upstream to Portal. The initial purpose is that Portal makes its own "local cache" of these _meta-information_ to drive "Pipeline Automation" and/or linking with Cloud buckets "File Object" purpose (i.e. `S3Object`, `GDSFile` indexes). Portal could have left, not to expose these "meta-info"; however, Portal exploration was the stepping stone case for future betterment; of how to best to handle these structures.
> 
> Portal keeps them in their own original "flat" data model for no join, "fast" read performance; i.e. as in [denormalized](https://www.google.com/search?q=denormalized) form. Portal use cases are in transactional sense and, it is ok (enough) to work with this form without re-shaping much. Furthermore and, since Portal does not bear ownership of metadata life cycle (read-only), it left out any additional treatments such as potential data re-modelling for future works.

### Google LIMS

Google LIMS update lambda is idempotent i.e. you can hit as many times as you like. Portal do `2-ways sync protocol` (merge & update) from Google LIMS sheet1.

> Strategy:  
> 1) append only of each sequencing samples upon BCL_Convert completion event (i.e. `GOOGLE_LIMS_UPDATE_STEP` in orchestrator)
> 2) merge & update each rows upon daily scheduled (for scenario when some modifications have happened)
> 3) not capturing any change history

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-lims_scheduled_update_processor \
  --invocation-type Event \
  lims_update.json
```

### Lab Metadata

Lab metadata update lambda is idempotent i.e. you can hit as many times as you like. Portal do `sync latest` (truncate & reload) from Google Lab tracking sheets.

> Strategy: 
> 1) truncate the table & re-import all sheets
> 2) not capturing any change history 

```
aws lambda invoke --profile prodops \
  --function-name data-portal-api-prod-labmetadata_scheduled_update_processor \
  --invocation-type Event \
  meta_update.json
```

Alternatively, you can use **SampleSheet Checker UI** as follows.

- Goto https://sscheck.umccr.org
- Login using umccr.org (i.e. usual Portal Login)
- Click button next to label "Sync Portal Metadata"


#### Automation Usage

The following columns from Lab metadata tracking sheets are mandatory for Portal Automation purpose.

```
LibraryID
SampleID
SubjectID
Type
Assay
Phenotype
OverrideCycles  (use in BCL_Convert)
Workflow
```
