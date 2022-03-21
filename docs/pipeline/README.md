# ICA Pipeline Automation v1

Typically, this is a fully automated pipeline with workflows orchestration _i.e. automation of [BioInformatics CWL workflow](https://github.com/umccr/cwl-ica) runs in ICA (Illumina Connected Analytics) WES (Workflow Execution Service)_. 

## TL;DR

- See [cleanup](cleanup) for pipeline failure, cleanup and rerun SOP.
- See [automation](automation) for operational notes.
- See [metadata.md](metadata.md) for syncing upstream Metadata.


## Concept

The current automation is centered around (workflow) events, i.e. actions are triggered by previous actions/events. As such particular workflows/steps can be triggered by replying/simulating external events.

More architecture details (at various stages) can be found in [this](https://lucid.app/lucidchart/e18f78ed-4132-4a5d-81f3-98d3b44936d4/edit?page=0_0#?folder_id=home&browser=icon) LucidChart document.

Since development is evolving fast, here is a simplified text diagram of the main steps of the pipeline automation.

```
                     UPDATE_STEP
                     FASTQ_UPDATE_STEP
                     GOOGLE_LIMS_UPDATE_STEP
BSSH > BCL_CONVERT > DRAGEN_WGS_QC_STEP        > TUMOR_NORMAL_STEP  > UMCCRISE_STEP > RNASUM_STEP
                     DRAGEN_TSO_CTDNA_STEP
                     DRAGEN_WTS_STEP
```

Automation entry-points are cloud-native serverless AWS Lambda compute unit -- an event controller. These controllers are event-driven and react to events delivered from ICA ENS event subscription through SQS. These lambdas can be seen as job/event consumers of a particular queue.

A central Orchestrator receives most external events and performs 2 simple tasks: 
1. update step
2. next step

This works like in feedback loop tandem such that:

- In each update step for a given workflow run, orchestrator perform updating _this_ workflow automation state in database.
- Then, it determines next step based on _this_ workflow status and invoke _step module's_ perform interface method.
- **Step module** is Job Producer and, it produces a job (or job list) into their respective queue. These main queues are then configured to **DLQ** for error handling and fault-tolerant purpose.
- Main queues are FIFO and deduplication by message content for distributed event control.
