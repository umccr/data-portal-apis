import json

from mockito import when

from data_portal.models import Workflow
from data_portal.tests.factories import TestConstant, OncoanalyserWgsWorkflowFactory
from data_processors.pipeline.lambdas import sqs_batch_event, orchestrator
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger

_output = {
    "output_directory": "s3://bucket/analysis_data/SBJ99999/oncoanalyser/20230911wgsaaaaa/wgs/L9999991__L9999992"
}

_parameters = {
    "portal_run_id": TestConstant.portal_run_id_oncoanalyser.value,
    "output": json.dumps(_output),
    "workflow": "oncoanalyser_wgs",
    "version": "v0.1.1"
}

_mock_batch_event = {
    "version": "0",
    "id": "eeeEe5fe-8044-93d3-0a87-198913fb479a",
    "detail-type": "Batch Job State Change",
    "source": "aws.batch",
    "account": "123456789012",
    "time": "2023-09-11T12:21:28Z",
    "region": "ap-southeast-2",
    "resources": [
        "arn:aws:batch:ap-southeast-2:123456789012:job/bbbBB8ce-b82d-4235-a878-72735337110e"
    ],
    "detail": {
        "jobArn": "arn:aws:batch:ap-southeast-2:123456789012:job/bbbBB8ce-b82d-4235-a878-72735337110e",
        "jobName": "oncoanalyser__wgs__SBJ99999__L9999991__L9999992__20230911wgsaaaaa",
        "jobId": "bbbBB8ce-b82d-4235-a878-72735337110e",
        "jobQueue": "arn:aws:batch:ap-southeast-2:123456789012:job-queue/nextflow-pipeline",
        "status": "SUCCEEDED",
        "attempts": [
            {
                "container": {
                    "containerInstanceArn": "arn:aws:ecs:ap-southeast-2:123456789012:container-instance/BaseargsserviceTypepipel-Pdsu4gTUtGFMIKOz_Batch_5aeb4ab6",
                    "taskArn": "arn:aws:ecs:ap-southeast-2:123456789012:task/BaseargsserviceTypepipel-Pdsu4gTUtGFMIKOz_Batch_5aeb4ab6-8131-3828-8151",
                    "exitCode": 0,
                    "logStreamName": "Nextflowoncoanalyser<snip>",
                    "networkInterfaces": []
                },
                "startedAt": 1694426212249,
                "stoppedAt": 1694434888327,
                "statusReason": "Essential container in task exited"
            }
        ],
        "statusReason": "Essential container in task exited",
        "createdAt": 1694426078485,
        "retryStrategy": {
            "attempts": 1,
            "evaluateOnExit": []
        },
        "startedAt": 1694426212249,
        "stoppedAt": 1694434888327,
        "dependsOn": [],
        "jobDefinition": "arn:aws:batch:ap-southeast-2:123456789012:job-definition/Nextflowoncoanalyser235-5ccb787d63a4854:1",
        "parameters": _parameters,
        "container": {
            "image": "123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/oncoanalyser:latest",
            "command": [
                "bash",
                "-o",
                "pipefail",
                "-c",
                "./assets/run.sh --portal_run_id 20230911wgsaaaaa --mode wgs --subject_id SBJ99999 --output_results_dir s3://temp-dev/analysis_data/SBJ99999/oncoanalyser/20230911wgsaaaaa/wgs/L9999991__L9999992 --output_staging_dir s3://temp-dev/temp_data/SBJ99999/oncoanalyser/20230911wgsaaaaa/scratch --output_scratch_dir s3://temp-dev/temp_data/SBJ99999/oncoanalyser/20230911wgsaaaaa/staging --tumor_wgs_sample_id MDX210176 --tumor_wgs_library_id L9999991 --tumor_wgs_bam s3://bucket-dev/oncoanalyser_test_data/SBJ99999/wgs/bam/GRCh38_umccr/MDX210176_tumor.bam --normal_wgs_sample_id MDX210175 --normal_wgs_library_id L9999992 --normal_wgs_bam s3://bucket-dev/oncoanalyser_test_data/SBJ99999/wgs/bam/GRCh38_umccr/MDX210175_normal.bam"
            ],
            "jobRoleArn": "arn:aws:iam::123456789012:role/NextflowApplicationDevSta-PipelineBatchInstanceRole",
            "volumes": [
                {
                    "host": {
                        "sourcePath": "/var/run/docker.sock"
                    },
                    "name": "docker_socket"
                }
            ],
            "environment": [
                {
                    "name": "AWS_REGION",
                    "value": "ap-southeast-2"
                },
                {
                    "name": "AWS_ACCOUNT",
                    "value": "123456789012"
                }
            ],
            "mountPoints": [
                {
                    "containerPath": "/var/run/docker.sock",
                    "readOnly": False,
                    "sourceVolume": "docker_socket"
                }
            ],
            "readonlyRootFilesystem": False,
            "ulimits": [],
            "privileged": False,
            "exitCode": 0,
            "containerInstanceArn": "arn:aws:ecs:ap-southeast-2:123456789012:container-instance/BaseargsserviceTypepipel-Pdsu4gTUtGFMIKOz_Batch_5aeb4ab6",
            "taskArn": "arn:aws:ecs:ap-southeast-2:123456789012:task/BaseargsserviceTypepipel-Pdsu4gTUtGFMIKOz_Batch_5aeb4ab6-8131-3828-8151",
            "logStreamName": "Nextflowoncoanalyser<snip>",
            "networkInterfaces": [],
            "resourceRequirements": [
                {
                    "value": "15000",
                    "type": "MEMORY"
                },
                {
                    "value": "2",
                    "type": "VCPU"
                }
            ],
            "secrets": []
        },
        "tags": {
            "resourceArn": "arn:aws:batch:ap-southeast-2:123456789012:job/bbbBB8ce-b82d-4235-a878-72735337110e"
        },
        "propagateTags": False,
        "platformCapabilities": [
            "EC2"
        ],
        "eksAttempts": []
    }
}

_mock_sqs_event = {
    "Records": [
        {
            "messageId": "aaaaAA0e-4b00-4a2e-a92e-bdaa98c3eeEE",
            "receiptHandle": "AQEBiFujdr<snip>96Y=",
            "body": json.dumps(_mock_batch_event),
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1694434888955",
                "SenderId": "AAAAAAAAARPI7CT46GGGG",
                "ApproximateFirstReceiveTimestamp": "1694434888964"
            },
            "messageAttributes": {},
            "md5OfBody": "ccCCCCCCCCc9<snip>",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:123456789012:data-portal-batch-event-queue",
            "awsRegion": "ap-southeast-2"
        }
    ]
}


class SQSBatchEventUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(SQSBatchEventUnitTests, self).setUp()
        self.verify_local()

    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.lambdas.tests.test_sqs_batch_event.SQSBatchEventUnitTests.test_handler
        """
        _ = OncoanalyserWgsWorkflowFactory()

        when(orchestrator).next_step(...).thenReturn({})  # do not advance to next step yet

        _ = sqs_batch_event.handler(event=_mock_sqs_event, context=None)

        stub_workflow = Workflow.objects.get(portal_run_id=TestConstant.portal_run_id_oncoanalyser.value)

        logger.info(f"-" * 32)
        logger.info(stub_workflow)
        logger.info(stub_workflow.output)

        self.assertIsNotNone(stub_workflow.output)
        self.assertIsInstance(stub_workflow.output, str)
        self.assertEqual(stub_workflow.end_status, "Succeeded")

        output_from_db = json.loads(stub_workflow.output)
        logger.info(output_from_db)
        self.assertEqual(output_from_db, _output)
