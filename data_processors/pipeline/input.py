# -*- coding: utf-8 -*-
"""Input module
High level pipeline related workflow specific input preparation
"""
import copy
import logging

from data_portal.models import SequenceRun

logger = logging.getLogger(__name__)


class BCLConvertInput(object):

    def __init__(self, input_tpl: dict, sqr: SequenceRun):
        gds_path = f"gds://{sqr.gds_volume_name}{sqr.gds_folder_path}"
        sample_sheet_gds_path = f"{gds_path}/SampleSheet.csv"
        self._bcl_input = copy.deepcopy(input_tpl)
        self._bcl_input['samplesheet-split']['location'] = sample_sheet_gds_path
        self._bcl_input['bcl-inDir']['location'] = gds_path

    @property
    def bcl_input(self) -> dict:
        return self._bcl_input

    def get_input(self) -> dict:
        return self._bcl_input


class GermlineInput(object):

    def __init__(self, input_tpl: dict, fastq1, fastq2, sample_id):
        self._germline_input = copy.deepcopy(input_tpl)
        self._germline_input['fq1-dragen']['location'] = fastq1
        self._germline_input['fq2-dragen']['location'] = fastq2
        self._germline_input['outdir-dragen'] = f"dragenGermline-{sample_id}"
        self._germline_input['rgid-dragen'] = f"{sample_id}"
        self._germline_input['rgsm-dragen'] = f"{sample_id}"
        self._germline_input['outprefix-dragen'] = f"{sample_id}"
        self._germline_input['outputDir-mulitqc'] = f"out-dir-{sample_id}"
        self._germline_input['subset-bam-name-sambamba'] = f"{sample_id}-subset.hla.bam"
        self._germline_input['sample-name'] = f"{sample_id}"
        self._germline_input['output-dirname'] = f"{sample_id}_HLA_calls"
        self._germline_input['outPrefix-somalier'] = f"{sample_id}"
        self._germline_input['outputDir-somalier'] = f"out-dir-{sample_id}"

    @property
    def germline_input(self) -> dict:
        return self._germline_input

    def get_input(self) -> dict:
        return self._germline_input
