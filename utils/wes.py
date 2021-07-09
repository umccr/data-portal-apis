import logging

from libica.openapi import libwes

from utils import ica, libjson

logger = logging.getLogger()


def get_run(wfr_id, to_dict=False, to_json=False):
    """
    Get workflow run from WES endpoint ala `ica workflows runs get wfr.81cf25dxxx`

    :param wfr_id:
    :param to_json: False by default
    :param to_dict: False by default
    :return: instance of libwes.WorkflowRun unless to_dict or to_json
    """

    with libwes.ApiClient(ica.configuration(libwes)) as api_client:
        run_api = libwes.WorkflowRunsApi(api_client)
        try:
            logger.info(f"Getting '{wfr_id}' from WES RUN API endpoint")
            wfl_run: libwes.WorkflowRun = run_api.get_workflow_run(run_id=wfr_id)

            if to_dict:
                return wfl_run.to_dict()

            elif to_json:
                return libjson.dumps(wfl_run.to_dict())

            return wfl_run
        except libwes.ApiException as e:
            logger.error(f"Exception when calling get_workflow_run: \n{e}")
