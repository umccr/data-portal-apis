# -*- coding: utf-8 -*-
"""orchestration package

This typically compliment Orchestrator act upon a particular event to PERFORM some step(s).
Typically, contain X_step modules file that implement "perform()" interface for a particular step in the pipeline.

Very Domain-rich impls!
Domain logic are mainly concentrated here. After domain logic are sorted here in X_step module, you use
service layer X_srv(s) to interface with RDBMS for persisting their state in database and/or notifying to Slack.

Not limited only to Orchestrator!
The whole point we do here is Genomic Workflow "orchestration" that is the key domain context.
Orchestrator (lambda) is the key actor. But other actors from lambdas package _steps_ are welcome here too!

Few internal behaviours
This denotes function start with underscore; in the following are shared (reusable) across step modules
i.e. consider this package __init__ module as "(Abstract) Parent Step" class (if you are OOP-er tinker)!

There might be a bit of friction between whether things should be here or, in liborca. You guess it right! Only subtle
difference and, guidelines here are:
 - if function is public modifier by nature and, its parameters can be deduced to primitive types then go to liborca
 - if it is internal behaviours that make sense only within step modules then they are here!
"""
from typing import List

import pandas as pd

from data_portal.models.fastqlistrow import FastqListRow
from data_portal.models.labmetadata import LabMetadata, LabMetadataPhenotype, LabMetadataType
from data_processors.pipeline.tools import liborca
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _reduce_and_transform_to_df(meta_list: List[LabMetadata]) -> pd.DataFrame:
    # also reduce to columns of interest
    return pd.DataFrame(
        [
            {
                "library_id": meta.library_id,
                "subject_id": meta.subject_id,
                "sample_id": meta.sample_id,
                "phenotype": meta.phenotype,
                "type": meta.type,
                "workflow": meta.workflow,
            } for meta in meta_list
        ]
    )


def _extract_unique_meta(meta_list_df: pd.DataFrame, column_prop: str) -> List[str]:
    if meta_list_df.empty:
        return []
    return meta_list_df[column_prop].unique().tolist()


def _extract_unique_subjects(meta_list_df: pd.DataFrame) -> List[str]:
    return _extract_unique_meta(meta_list_df, "subject_id")


def _extract_unique_libraries(meta_list_df: pd.DataFrame) -> List[str]:
    return _extract_unique_meta(meta_list_df, "library_id")


def _extract_unique_wgs_tumor_samples(meta_list_df: pd.DataFrame) -> List[str]:
    _df = meta_list_df.copy().loc[
        (meta_list_df['type'] == LabMetadataType.WGS.value) &
        (meta_list_df['phenotype'] == LabMetadataPhenotype.TUMOR.value)
        ]
    return _extract_unique_meta(_df, "sample_id")


def _mint_libraries(libraries):
    s = set()
    for lib in libraries:
        s.add(liborca.strip_topup_rerun_from_library_id(lib))
    return list(s)


def _handle_rerun(fastq_list_rows: List[FastqListRow], library_id):
    for fastq_list_row in fastq_list_rows:
        full_sample_library_id = fastq_list_row.rgid.rsplit(".", 1)[-1]
        if liborca.sample_library_id_has_rerun(full_sample_library_id):
            logger.warning(f"We found a rerun for library id {library_id} in {full_sample_library_id}. "
                           f"Please run this sample manually")
            # Reset fastq list rows - we don't know what to do with reruns
            return []
    return fastq_list_rows
