# Portal Metadata

Portal has data processing Lambdas for scheduled or on-demand sync with "upstream" metadata sources as "source-of-truth". 

> Design Note:
> 
> While Portal offers a convenience way to access these metadata through `/metadata` and `/lims` endpoints; or through Portal Athena; it is to note that "ownership" of these metadata and its structures are all upstream to Portal. The initial purpose is that Portal makes its own "local cache" of these _meta-information_ to drive "Pipeline Automation" and/or linking with Cloud buckets "File Object" purpose (i.e. `S3Object`, `GDSFile` indexes). Portal could have left, not to expose these "meta-info"; however, Portal exploration was the stepping stone case for future betterment; of how to best to handle these structures.
> 
> Portal keeps them in their own original "flat" data model for no join, "fast" read performance; i.e. as in [denormalized](https://www.google.com/search?q=denormalized) form. Portal use cases are in _read-only_ transactional sense and, it is ok (enough) to work with this form without re-shaping much. Furthermore and, since Portal does not bear ownership of metadata life cycle (read-only), it left out any additional treatments such as potential data re-modelling for future works.

## Google LIMS

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

## Lab Metadata

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


### Automation Usage

The following columns from Lab metadata tracking sheets are mandatory for Portal Automation purpose.

- Use in Secondary Analysis
```
LibraryID
SampleID
SubjectID
Type
Phenotype
Workflow
```

- Use in BCL_Convert
```
Assay
OverrideCycles
```

### LibraryID Suffixes

- Portal Automation removes any form of Lab annotated LibraryID suffixes before triggering Bioinformatics Secondary Analysis workflows.
- See [library_suffix.md](library_suffix.md)

### PTC and NTC

_(See https://umccr.slack.com/archives/CP356DDCH/p1697583381042659)_

- For WGS T/N cases,
	- We do not explicitly filter NTC/PTC yet (rule wise)
	- However, T/N pairing step only take in consider - `Phenotype`: `tumor` or `normal`
		- So, all NTC get filtered (inheritance)
	- PTC are still getting paired and processed
		- If, only if, LabMetadata `Workflow` column must be either of: `clinical` or `research`

- For WTS,
	- We do not explicitly filter NTC/PTC yet (rule wise)
	- However, for transcriptome workflow, we only consider - `Phenotype`: `tumor`
		- So, all NTC get filtered (inheritance)
	- PTC are still getting processed
		- If, only if, LabMetadata `Workflow` column must be either of: `clinical` or `research`

- For ctTSO,
	- We explicitly allow process NTC (https://github.com/umccr/data-portal-apis/issues/561)
	- PTC are getting processed

### New Meta Sheet

This activity typically happens at the end of year or, start of the year.

> What process involves when we add a new sheet into "Lab Metadata Tracking" Google spreadsheet?

1. Add a new sheet to Lab Metadata Tracking Google spreadsheet
2. Create Portal PR to make it aware. REF:
    - https://github.com/umccr/data-portal-apis/pulls?q=is%3Apr+is%3Aclosed+lab+metadata+sheet
    - https://github.com/umccr/data-portal-apis/pull/535
    - https://github.com/umccr/data-portal-apis/pull/653
3. Create Infrastructure PR to update SubjectID generator App script
    - https://github.com/umccr/infrastructure/blob/master/scripts/umccr_pipeline/UMCCR_Library_Tracking_MetaData.js#L5
    - https://github.com/umccr/infrastructure/pull/344
    - https://github.com/umccr/infrastructure/pull/391
4. Deploy SubjectID generator App script
5. Update SampleSheet Checker UI (if any)
   - https://github.com/umccr/samplesheet-check-frontend/pull/40
   - https://umccr.slack.com/archives/C94CQMNVA/p1704844116202929

We wish to improve this process. See https://trello.com/c/2i6PvmjT
