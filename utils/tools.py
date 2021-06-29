from utils import libjson


def parse_bcl_convert_output(output_json: str) -> list:
    """
    NOTE: as of BCL Convert CWL workflow version 3.7.5, it uses fastq_list_rows format
    Given bcl convert workflow output json, return fastq_list_rows
    See Example IAP Run > Outputs
    https://github.com/umccr-illumina/cwl-iap/blob/master/.github/tool-help/development/wfl.84abc203cabd4dc196a6cf9bb49d5f74/3.7.5.md

    :param output_json: workflow run output in json format
    :return fastq_list_rows: list of fastq list rows in fastq list format
    """
    output: dict = libjson.loads(output_json)

    lookup_keys = ['main/fastq_list_rows', 'fastq_list_rows']  # lookup in order, return on first found
    look_up_key = None
    for k in lookup_keys:
        if k in output.keys():
            look_up_key = k
            break

    if look_up_key is None:
        raise KeyError(f"Unexpected BCL Convert CWL output format. Expecting one of {lookup_keys}. Found {output.keys()}")

    return output[look_up_key]