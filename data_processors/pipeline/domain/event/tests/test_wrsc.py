import uuid
from datetime import datetime

from data_portal.fields import IdHelper
from data_processors.pipeline.domain.event.wrsc import WorkflowRunStateChangeEnvelope, WorkflowRunStateChange
from data_processors.pipeline.domain.workflow import WorkflowType
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class WRSCUnitTests(PipelineUnitTestCase):

    def setUp(self) -> None:
        super(WRSCUnitTests, self).setUp()

    def test_model_serde(self):
        """
        python manage.py test data_processors.pipeline.domain.event.tests.test_wrsc.WRSCUnitTests.test_model_serde
        """
        mock_obj = WorkflowRunStateChangeEnvelope(
            id=str(uuid.uuid4()),
            # detail_type="Workflow Run State Change",
            source="aws.batch",
            time=datetime.utcnow().isoformat(),
            detail=WorkflowRunStateChange(
                portal_run_id=IdHelper.generate_portal_run_id(),
                type_name=WorkflowType.STAR_ALIGNMENT.value.lower(),
                version="v0.1.0",
                output="{\"output_directory\": \"gds://somewhere/over/the/rainbow\"}",
                end_status="Succeeded",
                wfr_name="oncoanalyser__wgts__SBJ99999__L9999999__L9999998__L9999997__1693711089",
                wfr_id=str(uuid.uuid4()),
                wfv_id="arn:aws:batch:ap-southeast-2:123456789012:job-definition/0_mocking_echo_job:1",
                wfl_id="123456789012.dkr.ecr.ap-southeast-2.amazonaws.com/oncoanalyser:20230809025308--be57016e",
                end=datetime.utcnow()
            )
        )

        # test model serialization
        logger.info(dict(mock_obj))
        logger.info("-" * 128)
        logger.info(mock_obj.model_dump(by_alias=False))
        logger.info("-" * 128)
        logger.info(mock_obj.model_dump(by_alias=True))  # by_alias=True make it to `detail-type`
        logger.info("-" * 128)
        logger.info(mock_obj.model_dump_json(by_alias=True))
        self.assertIsNotNone(mock_obj)

        mock_event = mock_obj.model_dump(by_alias=True)
        logger.info(type(mock_event))
        self.assertIsInstance(mock_event, dict)  # assert that `mock_event` is some `dict` object

        # validate and deserialize from some dict object
        wrsc_envelope1 = WorkflowRunStateChangeEnvelope.model_validate(mock_event)
        logger.info(wrsc_envelope1.model_dump(by_alias=True))

        # unpack and deserialize from some dict object
        wrsc_envelope2 = WorkflowRunStateChangeEnvelope(**mock_event)
        logger.info(wrsc_envelope2.model_dump(by_alias=True))

        self.assertEqual(wrsc_envelope1, wrsc_envelope2)
