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
from dataclasses import dataclass, field
import logging
from abc import ABC, abstractmethod
from enum import Enum
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


class SomalierReferenceSite(Enum):
    HG38_RNA = "hg38.rna"
    HG19_RNA = "hg19.rna"

    @classmethod
    def from_value(cls, value):
        if value == cls.HG38_RNA.value:
            return cls.HG38_RNA
        elif value == cls.HG19_RNA.value:
            return cls.HG19_RNA
        else:
            raise ValueError(f"No matching type found for {value}")


@dataclass
class HolmesDto(ABC):
    """
    HolmesDto - Holmes Data Transfer Object (DTO) i.e. just _plain old python object_ (POPO)
    that model after Holmes payload data carrier
    """
    run_name: str
    indexes: List = field(default_factory=list)
    # fingerprint_folder: str = "fingerprints/"  # FIXME use Holmes default


@dataclass
class HolmesExtractDto(HolmesDto):
    reference: SomalierReferenceSite = SomalierReferenceSite.HG38_RNA


@dataclass
class HolmesCheckDto(HolmesDto):
    # relatedness_threshold: float = 0.4  # FIXME use Holmes default
    pass


class HolmesInterface(ABC):
    """Holmes Interface Contract

    Model after holmes execution interfaces
    https://github.com/umccr/holmes

    Holmes is the AWS step function orchestration of the somalier tool
    """

    @abstractmethod
    def extract(self, dto: HolmesDto):
        pass

    @abstractmethod
    def check(self, dto: HolmesDto):
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

        self.service_id = self.discover_service_id()
        self.service_attributes = self.discover_service_attributes()

        self.check_steps_arn = self.service_attributes[self.CHECK_STEPS_ARN_KEY]
        self.extract_steps_arn = self.service_attributes[self.EXTRACT_STEPS_ARN_KEY]

        self.execution_arn = None
        self.execution_instance = None
        self.execution_result = None

    def discover_service_id(self) -> str:
        fingerprint_service_id_list = list(
            filter(
                lambda x: x.get("Name") == self.SERVICE_NAME,
                self.srv_discovery_client.list_services().get("Services")
            )
        )

        if len(fingerprint_service_id_list) == 0:
            raise RuntimeError("Could not find the fingerprint services")

        return fingerprint_service_id_list[0].get("Id")

    def discover_service_attributes(self) -> Dict:
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

    def extract(self, dto: HolmesExtractDto):
        """payload bound to holmes extract interface
        https://github.com/umccr/holmes#extract
        Reference which defaults to hg38 has been added
        ctTSO will need to call extract with alternative reference
        """
        step_function_instance_obj = self.stepfn_client.start_execution(
            stateMachineArn=self.extract_steps_arn,
            name=dto.run_name,
            input=libjson.dumps({
                "indexes": dto.indexes,
                "reference": dto.reference.value,
                # "fingerprintFolder": dto.fingerprint_folder,  # FIXME use Holmes default
            })
        )

        self.execution_instance = step_function_instance_obj
        self.execution_arn = step_function_instance_obj['executionArn']
        return self

    def check(self, dto: HolmesCheckDto):
        """payload bound to holmes check interface
        https://github.com/umccr/holmes#check
        """
        step_function_instance_obj = self.stepfn_client.start_execution(
            stateMachineArn=self.check_steps_arn,
            name=dto.run_name,
            input=libjson.dumps(
                {
                    "indexes": dto.indexes,
                    # "relatednessThreshold": dto.relatedness_threshold,  # FIXME use Holmes default
                    # "fingerprintFolder": dto.fingerprint_folder,  # FIXME use Holmes default
                }
            )
        )

        self.execution_instance = step_function_instance_obj
        self.execution_arn = step_function_instance_obj['executionArn']
        return self


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
