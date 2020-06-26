import json
import logging
import re

from libiap.openapi import libgds

from data_portal.models import Workflow
from data_processors.pipeline.dto import FastQ
from data_processors.pipeline.eps import GDSInterface

logger = logging.getLogger(__name__)


def extract_fastq_sample_name(filename):
    """
    Extract sample_id from FASTQ file name based on BSSH FASTQ filename Naming Convention.
    https://emea.support.illumina.com/help/BaseSpace_OLH_009008/Content/Source/Informatics/BS/NamingConvention_FASTQ-files-swBS.htm
    :param filename:
    :return: sample_name or None
    """
    sample_name = re.split('_S[0-9]+_', filename)[0].rstrip('_')
    if sample_name == filename:  # i.e. can't split by regex rule
        return None
    return sample_name


class FastQBuilder(GDSInterface):
    """
    Build FastQ from bcl convert GDS output directory
    """

    def __init__(self, workflow: Workflow):
        super().__init__()
        assert workflow.output is not None, f"{workflow.type_name} {workflow.wfr_id} has no output yet"
        self._workflow = workflow

    def build(self) -> FastQ:
        fastq_output = self._workflow.output
        output_gds_path: str = json.loads(fastq_output)['main/fastqs']['location']
        path_elements = output_gds_path.replace("gds://", "").split("/")
        volume_name = path_elements[0]
        path = path_elements[1:]
        output_path = f"/{'/'.join(path)}"

        fastq = FastQ()
        fastq.volume_name = volume_name
        fastq.path = output_path
        fastq.gds_path = output_gds_path
        fastq_map = fastq.fastq_map

        with self.api_client:
            files_api = libgds.FilesApi(self.api_client)
            try:
                page_token = None
                while True:
                    file_list: libgds.FileListResponse = files_api.list_files(
                        volume_name=[volume_name],
                        path=[f"{output_path}/*"],
                        page_size=1000,
                        page_token=page_token,
                    )

                    for item in file_list.items:
                        file: libgds.FileResponse = item
                        if not file.name.endswith('.fastq.gz'):
                            continue
                        sample_name = extract_fastq_sample_name(file.name)
                        if sample_name:
                            if sample_name in fastq_map:
                                fastq_map[sample_name]['fastq_list'].append(file.name)
                            else:
                                fastq_map[sample_name]['fastq_list'] = [file.name]
                                fastq_map[sample_name]['tags'] = []  # TODO tag SBJ ID
                        else:
                            logger.info(f"Failed to extract sample name from file: {file.name}")

                    page_token = file_list.next_page_token
                    if not file_list.next_page_token:
                        break
            except libgds.ApiException as e:
                logger.error(f"Exception when calling list_files: \n{e}")

        return fastq
