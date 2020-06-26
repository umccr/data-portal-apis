# -*- coding: utf-8 -*-
"""Data Transfer Object module contains pure data classes or POPO (Plain-Old-Python-Object)
High level pipeline DTO data structure including enum for supplementing type definition
"""
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class WorkflowType(Enum):
    BCL_CONVERT = "bcl_convert"
    GERMLINE = "germline"


class WorkflowStatus(Enum):
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    ABORTED = "Aborted"


class FastQReadType(Enum):
    """Enum as in form of
    FRIENDLY_NAME_OF_SUPPORTED_FASTQ_READ_TYPE = NUMBER_OF_FASTQ_FILES_PRODUCE

    REF:
    FASTQ files explained
    https://sapac.support.illumina.com/bulletins/2016/04/fastq-files-explained.html?langsel=/au/

    bcl2fastq2 Conversion Software v2.20 User Guide
    https://sapac.support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html?langsel=/au/

    BCL Convert v3.4 User Guide
    """
    SINGLE_READ = 1
    PAIRED_END = 2
    PAIRED_END_TWO_LANES_SPLIT = 4  # i.e. bcl-convert (or bcl2fastq) run without --no-lane-splitting


@dataclass
class FastQ(object):
    """Data container class to hold FASTQ list in the map structure.
    Model specifically as data only class. So that it can be built and pass-around as data transfer object (dto).

    The fastq_map is backed by nested dict. At the moment, the fastq_map has the following structure.
    It is flexible, in a sense, the inner dict can get expanded as need be, i.e. act more like a bag to collect
    matching sample FASTQ files with easy recall sample_id as index. Optional tags for grouping purpose.
    {
        'SAMPLE_ID': {
            'fastq_list': ['SAMPLE_ID_S1_L001_R1_001.fastq.gz', 'SAMPLE_ID_S1_L001_R2_001.fastq.gz', ...],
            'tags': ['optional', 'aggregation', 'tag', 'for_example', 'SBJ00001', ...],
            ...
        },
        ...
    }

    AWS S3 and GDS friendly and it contains volume_name, path, gds_path when they are on GDS.
    Or, bucket, key, s3_path when they are on S3.

    See FastQBuilder for one way of building this FastQ, based on FASTQ output files from GDS.
    """

    def __init__(self):
        self.volume_name: Optional[str] = None
        self.path: Optional[str] = None
        self.gds_path: Optional[str] = None
        self.bucket: Optional[str] = None
        self.key: Optional[str] = None
        self.s3_path: Optional[str] = None
        self.fastq_map = defaultdict(dict)
