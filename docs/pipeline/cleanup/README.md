# Cleanup/Rerun SOP

## TL;DR

Depending on use case different cleanup steps should be performed.

### Whole Run

- Use `cleanup.sql`

### Specific Analysis Workflow

- Use `cleanup-analysis.sql`


## Different use cases

There are a couple of key concepts to keep in mind when rerunning/cleaning up workflows:

- If a workflow has failed outright, there should not be any output, so cleanup is generally not required.
- Workflow output is always contained in a workflow run specific ("folder") location, using the internal `portal_run_id` as part of the output path. Hence data is not overwritten. If previous data does exist it may be visible in the portal though and can cause confusion. Therefore a cleanup is recommended (ideally before new data is generated).
- Every workflow execution will generate DB entries, most will not affect reruns, some will auto-update, some will need manual update. This depends on the state the data is in when reruns happen.
- Generally workflow are rerun simulating/replaying the previous successful workflow/step, i.e. resume from the last successful/consistent state.

## Common scenarios

### Complete rerun of sequencing data

For each sequencing run the BCL Convert workflow is the first step. If this workflow has completed and needs to be rerun, a cleanup of the previously generated data is required.
Please follow the Whole Run instructions below to clean up DB records and workflow outputs.

### Partial rerun / rerun/resume of individual workflows

As previously stated, this is usually only required if previous attempts at running the workflow failed (and no results have been produced). Fixing the issue and re-triggering the workflow should suffice.
Consult the Specific Analysis Workflow case below.


### Steps

- Must have up-to-date understanding of Pipeline and Data Model
- General strategy: **preform cleanup ops before staging rerun trigger**
- Using SQL scripts to determine 
  - affected GDS paths to be deleted
  - affected workflows to be marked them `Failed`
  - affected DB metadata records deleted (`FastqListRow`s)
- When deleting GDS paths, GDS file events will fire and, Portal will auto-sync (remove them from) GDSFile index. Check DLQ for potentially missed GDS file events (as large number of events can overload the system temporarily).
- Request data admin (Florian) to delete them from GDS production volume.
- By design, `LibraryRun` and `FastqListRow` table will overwrite/update existing records, so a cleanup may not be required. However, if any related metadata has changed new DB records can be created instead of old ones updated (due to changing IDs). Therefore the safe approach is to clean up.
- Known issues that affect `FastqListRow`s overwrite are related to metadata that can change the `rgid` ID:
  - Sample Swap
  - `SampleSheet.csv` is mutated, since sequencing completed
  - Anything wrong with metadata annotations `Override Cycles`, `Assay`, `Type`, `Workflow`, `Phenotype`, Yield/QC `Coverage`

