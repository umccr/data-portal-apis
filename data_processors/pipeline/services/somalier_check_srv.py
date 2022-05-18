import logging
from typing import List, Dict
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SERVICE_NAME = "fingerprint"
CHECK_STEPS_ARN_KEY = "checkStepsArn"
EXTRACT_STEPS_ARN_KEY = "extractStepsArn"


def get_fingerprint_service_id() -> str:
    client = boto3.client('servicediscovery')

    fingerprint_service_id_list = list(
        filter(
               lambda x: x.get("Name") == SERVICE_NAME,
               client.list_services().get("Services")
               )
    )

    if len(fingerprint_service_id_list) == 0:
        logger.warning("Could not find the fingerprint services list")
        return None

    return fingerprint_service_id_list[0].get("Id")


def get_fingerprint_service_instances() -> Dict:
    client = boto3.client('servicediscovery')

    fingerprint_service_id = get_fingerprint_service_id()

    instances_list: List = client.list_instances(ServiceId=fingerprint_service_id).get("Instances", None)

    if instances_list is None or len(instances_list) == 0:
        logger.warning("Could not get instances list")
        return None

    attributes_dict: Dict = instances_list[0].get("Attributes", None)

    if attributes_dict is None:
        logger.warning("Could not get attributes list")
        return None

    return attributes_dict


def get_fingerprint_extraction_service_instance() -> str:
    service_instances: Dict = get_fingerprint_service_instances()

    if EXTRACT_STEPS_ARN_KEY in service_instances.keys():
        return service_instances.get(EXTRACT_STEPS_ARN_KEY)

    logger.warning("Could not find the fingerprint extraction service instance ID")
    return None


def get_fingerprint_check_service_instance() -> str:
    service_instances: Dict = get_fingerprint_service_instances()

    if CHECK_STEPS_ARN_KEY in service_instances.keys():
        return service_instances.get(CHECK_STEPS_ARN_KEY)

    logger.warning("Could not find the fingerprint extraction service instance ID")
    return None
