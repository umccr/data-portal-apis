# ICA Pipeline Automation v1

Typically, this is a fully automated pipeline with workflows orchestration _i.e. automation of [BioInformatics CWL workflow](https://github.com/umccr/cwl-ica) runs in ICA (Illumina Connected Analytics) WES (Workflow Execution Service)_. 

## TL;DR

- See [cleanup](cleanup) for pipeline failure, cleanup and rerun SOP.
- See [automation.md](automation.md) for operational note.


## Concept

We have developed in such that there are a particular point where we can invoke an AWS lambda function to start from a particular step in the pipeline. This offers so that we can resume the pipeline from that point onwards.

A better architecture diagram is available [here](https://lucid.app/lucidchart/e18f78ed-4132-4a5d-81f3-98d3b44936d4/edit?page=0_0#?folder_id=home&browser=icon).

Since development is evolving fast, here is a simplified text diagram for main pipeline automation.

```
                     UPDATE_STEP
                     FASTQ_UPDATE_STEP
                     GOOGLE_LIMS_UPDATE_STEP
BSSH > BCL_CONVERT > DRAGEN_WGS_QC_STEP        > TUMOR_NORMAL_STEP  > UMCCRISE_STEP > RNASUM_STEP
                     DRAGEN_TSO_CTDNA_STEP
                     DRAGEN_WTS_STEP
```

Automation entry-points are cloud-native serverless AWS Lambda compute unit -- an event controller. These controllers are Event-driven and Reactive upon ICA ENS subscription through SQS. They are a Job Consumer of a particular queue.

Orchestrator contains 2 simple interfaces: 
1. update step
2. next step

This works like in feedback loop tandem such that:

- In each update step for a given workflow run, orchestrator perform updating _this_ workflow automation state in database.
- Then, it determines next step based on _this_ workflow status and invoke _step module's_ perform interface method.
- **Step module** is Job Producer and, it produces a job (or job list) into their respective  queue. These main queues are then configured to **DLQ** for error handling and fault-tolerant purpose.
- Main queues are FIFO and deduplication by message content for distributed event control.
