# Cleanup SOP

### Steps

- Must up-to-date understanding with Pipeline Data Model
- General strategy: **preform cleanup ops before staging rerun trigger**
- Using SQL scripts to determine 
  - affected GDS paths to be deleted
  - affected workflows to be marked them `Failed`
- When deleting GDS paths, GDS file events will fire and, Portal will auto-sync (remove them from) GDSFile index.
- Request data admin (Florian) to delete them from GDS production volume.
- By design, `LibraryRun` and `FastqListRow` table will always point to the latest workflow run FASTQs output. Hence, no proactive cleanup is required.
- However, if there exist issue with critical meta info rotation at primary BCL conversion step, then it deems cleanup before staging rerun. Issues such as:
  - Sample Swap
  - `SampleSheet.csv` is mutated, since sequencing completed
  - Anything wrong with metadata annotations `Override Cycles`, `Assay`, `Type`, `Workflow`, `Phenotype`, Yield/QC `Coverage`

### Whole Run

- Use `cleanup.sql`

### Specific Analysis Workflow

- Use `cleanup-analysis.sql`
