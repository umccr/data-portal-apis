import json
from datetime import datetime, timedelta

from django.utils.timezone import make_aware

from data_portal.models import Workflow, LIMSRow, LabMetadata, SequenceRun
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.constant import WorkflowType, WorkflowStatus
from data_processors.pipeline.orchestration import google_lims_update_step
from data_processors.pipeline.services import workflow_srv
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import PipelineUnitTestCase, PipelineIntegrationTestCase
from data_processors.pipeline.tools import liborca

lc_no = 29  # number of lims columns
lc_idx_run_name = 0  # index of IlluminaID/RunName column
lc_idx_workflow = 21  # index of the Workflow column

mock_run_name = TestConstant.sqr_name.value  # "200508_A01052_0001_BH5LY7ACGT"
mock_workflow_type = "manual"
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


def create_mock_lims_row() -> LIMSRow:
    return LIMSRow(
        illumina_id=mock_run_name,
        run="100",
        timestamp="2021-01-01",
        subject_id="SBJ00100",
        sample_id=mock_sample_id,
        library_id=mock_library_id,
        sample_name=f"{mock_sample_id}_{mock_library_id}",
        project_owner="FOO",
        project_name="FooBar",
        type="WGS",
        assay="TsqNano",
        override_cycles="Y151;I8;I8;Y151",
        phenotype="tumor",
        source="tissue",
        quality="good",
        workflow=mock_workflow_type
    )


def create_mock_workflow(id: str) -> Workflow:
    mock_workflow = Workflow()
    mock_workflow.wfr_id = id
    mock_workflow.type_name = WorkflowType.BCL_CONVERT.value
    mock_workflow.end_status = WorkflowStatus.SUCCEEDED.value
    mock_workflow.output = json.dumps(mock_workflow_output)

    return mock_workflow


class GoogleLimsUpdateStepUnitTests(PipelineUnitTestCase):

    def test_convert_limsrow_to_tuple(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_google_lims_update_step.GoogleLimsUpdateStepUnitTests.test_convert_limsrow_to_tuple
        """
        lims_row: LIMSRow = create_mock_lims_row()
        lims_tuple: tuple = google_lims_update_step.convert_limsrow_to_tuple(limsrow=lims_row)

        self.assertEqual(len(lims_tuple), lc_no)
        self.assertEqual(lims_tuple[lc_idx_run_name], mock_run_name)
        self.assertEqual(lims_tuple[lc_idx_workflow], mock_workflow_type)

    def test_get_libs_from_run(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_google_lims_update_step.GoogleLimsUpdateStepUnitTests.test_get_libs_from_run
        """
        mock_workflow = create_mock_workflow(f"wfr.{_rand(32)}")
        mock_sqr: SequenceRun = SequenceRunFactory()
        mock_sqr.name = mock_run_name
        mock_workflow.sequence_run = mock_sqr

        res = google_lims_update_step.get_libs_from_run(mock_workflow)

        self.assertTrue(isinstance(res, list))
        self.assertEqual(len(res), 2)
        for rec in res:
            self.assertTrue(rec['id'] in [mock_library_id, mock_library_id_2], f"Unexpected lib id: {rec['id']}")
            self.assertTrue(rec['lane'] in [1, 2], f"Unexpected lane: {rec['lane']}")
            self.assertEqual(rec['run_name'], mock_run_name, f"Unexpected run name: {rec['run_name']}")

    def test_create_lims_entry(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_google_lims_update_step.GoogleLimsUpdateStepUnitTests.test_create_lims_entry
        """
        mock_lab_meta: LabMetadata = LabMetadata(
            library_id=mock_library_id,
            sample_id=mock_sample_id,
            sample_name=mock_rgms_1,
            phenotype="tumor",
            quality="good",
            source="tissue",
            type="WGS",
            assay="TsqNano",
            workflow=mock_workflow_type,
        )
        mock_lab_meta.save()

        lims_row: LIMSRow = google_lims_update_step.create_lims_entry(mock_library_id, mock_run_name)

        self.assertEqual(lims_row.illumina_id, mock_run_name)
        self.assertEqual(lims_row.library_id, mock_library_id)
        self.assertEqual(lims_row.sample_id, mock_sample_id)
        self.assertEqual(lims_row.workflow, mock_workflow_type)
        self.assertEqual(lims_row.run, liborca.get_run_number_from_run_name(mock_run_name))
        self.assertEqual(lims_row.timestamp, liborca.get_timestamp_from_run_name(mock_run_name))

    def test_get_workflow_for_seq_run_name(self):
        """
        python manage.py test data_processors.pipeline.orchestration.tests.test_google_lims_update_step.GoogleLimsUpdateStepUnitTests.test_get_workflow_for_seq_run_name
        """
        # persist a mock SequenceRun that is shared by all workflows
        mock_sqr: SequenceRun = SequenceRunFactory()
        mock_sqr.name = mock_run_name
        mock_sqr.save()

        # create a first workflow to test the normal case
        # where we have a 1:1 mapping of sequence run name to workflow runs
        wf = create_mock_workflow("wfr.12345")
        start = datetime.utcnow() - timedelta(days=2)
        wf.start = make_aware(start)
        wf.end = make_aware(start + timedelta(days=1))
        wf.sequence_run = mock_sqr
        wf.save()

        wf_res = workflow_srv.get_workflow_for_seq_run_name(mock_run_name)
        self.assertEqual(wf_res.sequence_run.name, mock_run_name)
        self.assertEqual(wf_res.type_name, WorkflowType.BCL_CONVERT.value)
        self.assertEqual(wf_res.end_status, WorkflowStatus.SUCCEEDED.value)

        # now add a second (newer) workflow for the same sequence run to test the case
        # where we have more than one workflow runs for the same sequence run name
        wf = create_mock_workflow("wfr.67890")
        start = datetime.utcnow() - timedelta(days=1)
        wf.start = make_aware(start)
        wf.end = make_aware(start + timedelta(days=1))
        wf.sequence_run = mock_sqr
        wf.save()

        wf_res = workflow_srv.get_workflow_for_seq_run_name(mock_run_name)
        self.assertEqual(wf_res.sequence_run.name, mock_run_name)
        self.assertEqual(wf_res.type_name, WorkflowType.BCL_CONVERT.value)
        self.assertEqual(wf_res.end_status, WorkflowStatus.SUCCEEDED.value)
        # we need to have the newer workflow (wfr.67890)
        self.assertEqual(wf_res.wfr_id, "wfr.67890")

        # add a third backdated workflow just to test that
        # we are not picking up the creation time/order (instead of the intended end time)
        wf = create_mock_workflow("wfr.01234")
        start = datetime.utcnow() - timedelta(days=10)
        wf.start = make_aware(start)
        wf.end = make_aware(start + timedelta(days=1))
        wf.sequence_run = mock_sqr
        wf.save()

        wf_res = workflow_srv.get_workflow_for_seq_run_name(mock_run_name)
        self.assertEqual(wf_res.wfr_id, "wfr.67890")  # we are still expecting wfr.67890, as it's the latest workflow


class GoogleLimsUpdateStepIntegrationTests(PipelineIntegrationTestCase):
    # integration test hit actual File or API endpoint, thus, manual run in most cases
    # required appropriate access mechanism setup such as active aws login session
    # uncomment @skip and hit the each test case!
    # and keep decorated @skip after tested

    pass
