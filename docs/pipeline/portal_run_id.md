# Portal Run ID

You may come across people mentioning this term. The following document and explain about its attributes, property, constraint rule and use cases that intend to solve.

> In summary, the `portal_run_id` is formula-based and, systematic scheme for herding the analysis run records into Portal Pipeline database; either with data "late binding" process or automated tracking process; so that one person (or a system event) would start the analysis without the wait and, still be consistent by following standard operation procedure (SOP) and best practises.

![portal_run_id.png](../model/portal_run_id.png)

link: `docs/model/portal_run_id.png`

## Curation

### Where would you encounter?

Often written as `portal_run_id` in Portal UI, [API endpoints](../ENDPOINTS.md) or data export from Portal database through [Portal Athena](../athena) or via meeting/Slack/Trello communication. This ID keeps track of every BioInformatics analysis run; also known as a "workflow run" for short. You may encounter this ID scheme when someone referring to analysis output for traceability with Portal ecosystem.

### Unit of work

It is designed in mind such that
- Every BioInformatics analysis run is immutable and single unit of work transaction
- Every BioInformatics analysis run get assigned one unique `portal_run_id`
  - This includes re-running the analysis for the same Sample other everytime
- Each analysis run must meet their terminal state such as `Failed`, `Aborted`, `Succeeded`, `Deleted`

A typical BioInformatics workflow (languages like [CWL](https://www.commonwl.org), [WDL](https://openwdl.org), [Nextflow](https://www.nextflow.io), [Snakemake](https://github.com/snakemake/snakemake)) consist of multiple steps or tasks. A Portal orchestration views one analysis workflow run as single unit of work transaction and, this workflow must either be completed successfully or, failed otherwise.

The complexity of a workflow contains many steps or, small steps within it; is upto the author of BioInformatics workflow expert concern. i.e. A workflow being too big or small. Regardless, Portal orchestration engine takes single workflow run as one unit of work item that must meet terminal condition without tampering the workflow execution run, _in-flight_.

### History

Deletion marker happens on the `portal_run_id` database record when the analysis run outcome found to be unfit for scientific curation by post BioInformatics analysis quality control process. When deletion request happens, the following Portal SOP will be carried out: 
- Update the corresponding `portal_run_id` status in Portal database to `Deleted`
- Delete the associated BioInformatics analysis results (artifacts such as html, pdf reports, csv/tsv, vcf, bam, etc) from the data storage (Cloud Object Store such as AWS S3)

Since Portal never delete "the contextual fact" about BioInformatics analysis run has been performed; by knowing `portal_run_id` (even it has deletion marker or soft-deletion marker), Portal can answer the input and, output of the analysis run at all time. This gives analysis run history by design for audit trail.

## BioInformatics

### Construct

The `portal_run_id` consists of two parts: **time component** and **random component**. It has total length of fixed size `16` characters. The ID construct algorithm as follows.

Step 1: Generate datestamp in `YYYYMMDD` format
```bash
$ date +"%Y%m%d"
20240512
```

Step 2: Generate UUID and get the first 8 characters block
```bash
uuidgen | cut -c 1-8
9e44e5e0
```

Step 3: Concatenation of the two components form the `portal_run_id`
```
202405129e44e5e0
```

**Justification**: The reasons why we do this ID construct scheme are as follows.
- It has to design in mind such that by looking at it, our end user can understand the analysis run date that has been performed.
- It has to be globally unique ID (at least within Portal system domain context).
- It is a formula-based ID such that it can be generated outside of Portal with minimal tool required.
- The ID must handle analysis run workload in "distributed fashion" (explain next).

### In-Band and Out-of-Band

When BioInformatics analysis runs are launched through Portal (exposed as AWS Lambda function calling [entrypoints](automation)), Portal internally assign the `portal_run_id` to start tracking of the analysis run. This use case is called "In-Band". Portal Automation then know how to update of the corresponding workflow run and, what to trigger next. Because the `portal_run_id` is being generated "In-Band" and, Portal know what to do with based on best practise BioInformatics runbook **rules** that has coded into the system.

The "Out-of-Band" analysis runs are happened outside the Portal Automation. Typical use cases are preparing some large Cohort (re)analysis. We request Bioinformatician to generate and assign every analysis workflow run execution to have one unique `portal_run_id`; including, even if, failed ones. This `portal_run_id` may have corresponding execution ID where the analysis get computed. For instance,

- If BioInformatics analysis workflow run on AWS Batch execution engine, you have a pair of `portal_run_id` and AWS Batch execution ID.
- If analysis workflow run on AWS StepFunction, you have a pair of `portal_run_id` and AWS StepFunction execution ID.
- If the workflow run on ICA WES (workflow execution service), you get `portal_run_id` and corresponding `wfr.<ID>`.
- If it is run on HPC systems (such Gadi, Spartan), you get `portal_run_id` and your Job ID (PBS, or queuing scheduler system).

So on so forth. 

Every execution of a workflow; you must generate a unique `portal_run_id` and, keep the record of corresponding execution ID from the underlay compute system. _This can even be on your local computer execution!_ (explain next)

### Output Path

As for Bioinformatician (as well as) anyone who perform the informatics analysis run; one should use this `portal_run_id` in the workflow result output path. Example such as:

```bash
/scratch/runs/wgs_tumor_normal/202405129e44e5e0/
file://scratch/runs/wgs_tumor_normal/202405129e44e5e0/
s3://scratch-bucket/runs/wgs_tumor_normal/202405129e44e5e0/
gds://scratch-volume/runs/wgs_tumor_normal/202405129e44e5e0/
icav2://scratch-project/runs/wgs_tumor_normal/202405129e44e5e0/
```

Once the analysis run has been concluded, we can drive the results back; enter into the Portal Pipeline database for traceability and/or further data warehousing purpose. Often, this process need to comply with Portal [Metadata](metadata.md).

In summary, the `portal_run_id` is formula-based and, systematic scheme for herding the analysis run records into Portal Pipeline database; either with data "late binding" process or automated tracking process; so that one person (or a system event) would start the analysis without the wait and, still be consistent by following standard operation procedure (SOP) and best practises.
