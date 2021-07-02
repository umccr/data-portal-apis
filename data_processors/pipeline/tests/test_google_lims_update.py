import json
from datetime import datetime, timedelta
from unittest import skip

from django.utils.timezone import make_aware

from data_portal.models import Workflow, LabMetadata, SequenceRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.lambdas import google_lims_update
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase

lc_no = 29  # number of lims columns

mock_run_name = TestConstant.sqr_name.value  # "200508_A01052_0001_BH5LY7ACGT"
mock_sample_id = "PRJ123456"
mock_library_id = "L2100021"
mock_library_id_2 = "L2100021_topup"
mock_rgms_1 = f"{mock_sample_id}_{mock_library_id}"
mock_rgms_2 = f"{mock_sample_id}_{mock_library_id_2}"
mock_workflow_output = {
    "main/fastq_list_rows": [
        {
            "rgid": "ACTAAGAT.CCGCGGTT.1.foo",
            "rglb": "UnknownLibrary",
            "rgsm": mock_rgms_1,
            "lane": 1,
            "read_1": {},
            "read_2": {}
        },
        {
            "rgid": "GTCGGAGC.TTATAACC.1.bar",
            "rglb": "UnknownLibrary",
            "rgsm": mock_rgms_2,
            "lane": 2,
            "read_1": {},
            "read_2": {}
        }
    ]
}


def create_mock_workflow(id: str) -> Workflow:
    mock_workflow = Workflow()
    mock_workflow.wfr_id = id
    mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
    mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
    mock_workflow.output = json.dumps(mock_workflow_output)

    return mock_workflow


class GoogleLimsUpdateUnitTests(PipelineUnitTestCase):

    pass


class GoogleLimsUpdateIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    @skip
    def test_handler(self):
        """
        python manage.py test data_processors.pipeline.tests.test_google_lims_update.GoogleLimsUpdateIntegrationTests.test_handler
        """
        # create mock entries
        wfr_id = "wfr.0011223344"

        mock_sqr: SequenceRun = SequenceRunFactory()
        mock_sqr.name = mock_run_name
        mock_sqr.save()

        wf = create_mock_workflow(wfr_id)
        wf.sequence_run = mock_sqr
        start = datetime.utcnow() - timedelta(days=1)
        wf.start = make_aware(start)
        wf.end = make_aware(start + timedelta(days=1))
        wf.save()

        # create LabMetadata entries for the libraries referenced in the workflow output
        lm1 = LabMetadata(
            library_id=mock_library_id,
            sample_name=mock_rgms_1,
            sample_id=mock_sample_id,
            external_sample_id=f"extid-{mock_sample_id}",
            subject_id="SBJ99999",
            external_subject_id="ext-SBJ99999",
            phenotype="tumor",
            quality="good",
            source="tissue",
            project_name="FOO",
            project_owner="FooBar",
            experiment_id="exp_id_123",
            type="WGS",
            assay="TsqNano",
            override_cycles="F:F:F:F",
            workflow="clinical",
            coverage="80",
            truseqindex="noidea",
        )
        lm1.save()

        lm2 = LabMetadata(
            library_id=mock_library_id_2,
            sample_name=mock_rgms_2,
            sample_id=mock_sample_id,
            external_sample_id=f"extid-{mock_sample_id}",
            subject_id="SBJ99999",
            external_subject_id="ext-SBJ99999",
            phenotype="tumor",
            quality="good",
            source="tissue",
            project_name="FOO",
            project_owner="FooBar",
            experiment_id="exp_id_123",
            type="WGS",
            assay="TsqNano",
            override_cycles="F:F:F:F",
            workflow="clinical",
            coverage="80",
            truseqindex="noidea",

        )
        lm2.save()

        event = {
            "wfr_id": wfr_id
        }
        resp = google_lims_update.handler(event=event, context=None)
        print(resp)
        # we expect two rows to be added (according to the LabMetadata above (lm1, lm2)
        self.assertEqual(resp['updates']['updatedRows'], 2)
        # the Google sheet contains lc_no (29) populated columns
        self.assertEqual(resp['updates']['updatedColumns'], lc_no)
