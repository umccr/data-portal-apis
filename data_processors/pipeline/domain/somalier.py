# -*- coding: utf-8 -*-
"""somalier domain module

Domain models related to Somalier Check of bam files
This is typically used to test the similarity between two bam files

Impl note:
Somalier Domain service for interfacing with (external system) Holmes pipeline
Using fluent interface pattern where applicable

See domain package __init__.py doc string.
See orchestration package __init__.py doc string.
"""
import logging
from abc import ABC, abstractmethod
from time import sleep
from typing import List, Dict

from libumccr import aws, libjson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SomalierInterface(ABC):
    """Somalier Interface Contract

    See https://github.com/brentp/somalier

    $./somalier --help
    Commands:
      extract      :   extract genotype-like information for a single sample from VCF/BAM/CRAM.
      relate       :   aggregate `extract`ed information and calculate relatedness among samples.
      ancestry     :   perform ancestry prediction on a set of samples, given a set of labeled samples
      find-sites   :   create a new sites.vcf.gz file from a population VCF (this is rarely needed).

    Impls note:
        At the mo, Holmes encapsulates Somalier. Hence, we use Somalier through Holmes interfaces.
        This native SomalierInterface is just a placeholder for now.
        And for future use i.e. to supplement /somalier endpoints for these native behaviours, if any.
    """

    @abstractmethod
    def extract(self, **kwargs):
        pass

    @abstractmethod
    def relate(self, **kwargs):
        pass

    @abstractmethod
    def ancestry(self, **kwargs):
        pass

    @abstractmethod
    def find_sites(self, **kwargs):
        pass


class HolmesInterface(ABC):
    """Holmes Interface Contract

    Model after holmes execution interfaces
    https://github.com/umccr/holmes

    Holmes is the AWS step function orchestration of the somalier tool
    """

    @abstractmethod
    def diff(self, **kwargs):
        """difference"""
        pass

    @abstractmethod
    def extract(self, **kwargs):
        pass

    @abstractmethod
    def check(self, **kwargs):
        pass

    @abstractmethod
    def diff_extract(self, **kwargs):
        """difference_then_extract"""
        pass


class HolmesPipeline(HolmesInterface):
    """
    A wrapper impl using Boto3.
    """

    SERVICE_NAME = "fingerprint"
    CHECK_STEPS_ARN_KEY = "checkStepsArn"
    EXTRACT_STEPS_ARN_KEY = "extractStepsArn"

    def __init__(self):
        self.srv_discovery_client = aws.srv_discovery_client()
        self.stepfn_client = aws.stepfn_client()

        self.service_id = self.__discover_service_id()
        self.service_attributes = self.__discover_service_attributes()

        self.check_steps_arn = self.service_attributes[self.CHECK_STEPS_ARN_KEY]
        self.extract_steps_arn = self.service_attributes[self.EXTRACT_STEPS_ARN_KEY]

        self.execution_arn = None
        self.execution_instance = None
        self.execution_result = None

    def __discover_service_id(self) -> str:
        fingerprint_service_id_list = list(
            filter(
                lambda x: x.get("Name") == self.SERVICE_NAME,
                self.srv_discovery_client.list_services().get("Services")
            )
        )

        if len(fingerprint_service_id_list) == 0:
            raise RuntimeError("Could not find the fingerprint services")

        return fingerprint_service_id_list[0].get("Id")

    def __discover_service_attributes(self) -> Dict:
        instances: List = self.srv_discovery_client.list_instances(ServiceId=self.service_id).get("Instances", None)

        if instances is None or len(instances) == 0:
            raise RuntimeError(f"Could not list-instances for service-id: {self.service_id}")

        attributes_dict: Dict = instances[0].get("Attributes", None)

        if attributes_dict is None:
            raise RuntimeError("Could not get attributes list")

        return attributes_dict

    def poll(self):
        """start polling step function execution_arn
        this assumes such that Holmes (somalier) execution is very fast! And it should be... :)
        """
        while True:
            execution_dict = self.stepfn_client.describe_execution(executionArn=self.execution_arn)
            if execution_dict['status'] != "RUNNING":
                break
            logger.info(f"Execution still running, sleeping 3")
            sleep(3)

        self.execution_result = execution_dict
        return self

    def extract(self, instance_name, gds_path):
        """payload bound to holmes extract interface
        https://github.com/umccr/holmes#extract
        """
        step_function_instance_obj = self.stepfn_client.start_execution(
            stateMachineArn=self.extract_steps_arn,
            name=instance_name,
            input=libjson.dumps({
                "needsFingerprinting": [
                    [
                        gds_path
                    ]
                ]
            })
        )

        self.execution_instance = step_function_instance_obj
        self.execution_arn = step_function_instance_obj['executionArn']
        return self

    def check(self, instance_name, index_path):
        """payload bound to holmes check interface
        https://github.com/umccr/holmes#check
        """
        step_function_instance_obj = self.stepfn_client.start_execution(
            stateMachineArn=self.check_steps_arn,
            name=instance_name,
            input=libjson.dumps(
                {
                    "index": index_path
                }
            )
        )

        self.execution_instance = step_function_instance_obj
        self.execution_arn = step_function_instance_obj['executionArn']
        return self

    def diff(self, **kwargs):
        raise NotImplementedError

    def diff_extract(self, **kwargs):
        raise NotImplementedError


class HolmesProxyImpl(HolmesInterface):
    """Proxy for REST endpoints <> Lambdas"""

    def __init__(self, payload=None):
        super().__init__()
        self._input = payload
        self._output = {}

    def diff(self):
        raise NotImplementedError

    def extract(self):
        from data_processors.pipeline.lambdas import somalier_extract
        self._output = somalier_extract.handler(self._input, context=None)
        return self

    def check(self):
        from data_processors.pipeline.lambdas import somalier_check
        self._output = somalier_check.handler(self._input, context=None)
        return self

    def diff_extract(self):
        raise NotImplementedError

    @property
    def output(self):
        return self._output.copy()


class SomalierImpl(SomalierInterface):

    def __init__(self):
        pass

    def extract(self):
        raise NotImplementedError

    def relate(self):
        raise NotImplementedError

    def ancestry(self):
        raise NotImplementedError

    def find_sites(self):
        raise NotImplementedError
