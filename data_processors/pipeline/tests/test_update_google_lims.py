import json

from data_portal.models import Workflow, LIMSRow, LabMetadata
from data_portal.tests.factories import SequenceRunFactory, TestConstant
from data_processors.pipeline.lambdas import update_google_lims
from data_processors.pipeline.tests import _rand
from data_processors.pipeline.tests.case import PipelineUnitTestCase


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


def create_mock_workflow() -> Workflow:

    mock_sqr = SequenceRunFactory()

    mock_workflow = Workflow()
    mock_workflow.wfr_id = f"wfr.{_rand(32)}"
    mock_workflow.sequence_run = mock_sqr
    mock_workflow.output = json.dumps(mock_workflow_output)

    return mock_workflow


class UpdateGoogleLimsUnitTests(PipelineUnitTestCase):

    def test_get_run_number_from_run_name(self):
        """
        python manage.py test data_processors.pipeline.tests.test_update_google_lims.UpdateGoogleLimsUnitTests.test_get_run_number_from_run_name
        """
        run_no = update_google_lims.get_run_number_from_run_name(mock_run_name)
        self.assertEqual(run_no, 1)

    def test_get_timestamp_from_run_name(self):
        """
        python manage.py test data_processors.pipeline.tests.test_update_google_lims.UpdateGoogleLimsUnitTests.test_get_timestamp_from_run_name
        """
        run_date = update_google_lims.get_timestamp_from_run_name(mock_run_name)
        self.assertEqual(run_date, "2020-05-08")

    def test_convert_limsrow_to_tuple(self):
        """
        python manage.py test data_processors.pipeline.tests.test_update_google_lims.UpdateGoogleLimsUnitTests.test_convert_limsrow_to_tuple
        """
        lims_row: LIMSRow = create_mock_lims_row()
        lims_tuple: tuple = update_google_lims.convert_limsrow_to_tuple(limsrow=lims_row)

        self.assertEqual(len(lims_tuple), lc_no)
        self.assertEqual(lims_tuple[lc_idx_run_name], mock_run_name)
        self.assertEqual(lims_tuple[lc_idx_workflow], mock_workflow_type)

    def test_get_libs_from_run(self):
        """
        python manage.py test data_processors.pipeline.tests.test_update_google_lims.UpdateGoogleLimsUnitTests.test_get_libs_from_run
        """
        mock_workflow = create_mock_workflow()
        res = update_google_lims.get_libs_from_run(mock_workflow)

        self.assertTrue(isinstance(res, list))
        self.assertEqual(len(res), 2)
        for rec in res:
            self.assertTrue(rec['id'] in [mock_library_id, mock_library_id_2], f"Unexpected lib id: {rec['id']}")
            self.assertTrue(rec['lane'] in [1, 2], f"Unexpected lane: {rec['lane']}")
            self.assertEqual(rec['run_name'], mock_run_name, f"Unexpected run name: {rec['run_name']}")

    def test_create_lims_entry(self):
        """
        python manage.py test data_processors.pipeline.tests.test_update_google_lims.UpdateGoogleLimsUnitTests.test_create_lims_entry
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

        lims_row: LIMSRow = update_google_lims.create_lims_entry(mock_library_id, mock_run_name)

        self.assertEqual(lims_row.illumina_id, mock_run_name)
        self.assertEqual(lims_row.library_id, mock_library_id)
        self.assertEqual(lims_row.sample_id, mock_sample_id)
        self.assertEqual(lims_row.workflow, mock_workflow_type)
        self.assertEqual(lims_row.run, update_google_lims.get_run_number_from_run_name(mock_run_name))
        self.assertEqual(lims_row.timestamp, update_google_lims.get_timestamp_from_run_name(mock_run_name))
