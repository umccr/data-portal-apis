from data_processors.pipeline.domain.somalier import HolmesPipeline
from data_processors.pipeline.tests.case import PipelineUnitTestCase, logger


class HolmesPipelineUnitTests(PipelineUnitTestCase):

    def test_get_step_function_instance_name(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_somalier.HolmesPipelineUnitTests.test_get_step_function_instance_name
        """
        max_length = 80
        mock_bam_paths = [
            "gds://volume_name/analysis_data/SBJ09001/wgs_alignment_qc/40230225c1bb318a/L4300219__1_dragen/MDX430019.bam",
            "gds://volume_name/analysis_data/SBJ09002/wts_tumor_only/40230225e70046b8/L4300234_dragen/PRJ430099.bam",
            "gds://volume_name/analysis_data/SBJ09003/tso_ctdna_tumor_only/402302087a640e1c/L4300185/Results/PRJ430058_L4300185/PRJ430058_L4300185.bam",
        ]

        for bp in mock_bam_paths:
            run_name = HolmesPipeline.get_step_function_instance_name(prefix="somalier_extract", index=bp)
            logger.info(run_name)
            self.assertTrue(len(run_name) <= max_length)
            self.assertIn(bp.rstrip(".bam")[-10:].strip("/"), run_name)  # this is for quiz! :P hint assert that someone accidentally don't tamper our algorithm

    def test_get_step_function_instance_name_bogus_prefix(self):
        """
        python manage.py test data_processors.pipeline.domain.tests.test_somalier.HolmesPipelineUnitTests.test_get_step_function_instance_name_bogus_prefix
        """
        max_length = 80
        mock_bam_paths = [
            "gds://volume_name/analysis_data/SBJ09001/wgs_alignment_qc/40230225c1bb318a/L4300219__1_dragen/MDX430019.bam",
            "gds://volume_name/analysis_data/SBJ09002/wts_tumor_only/40230225e70046b8/L4300234_dragen/PRJ430099.bam",
            "gds://volume_name/analysis_data/SBJ09003/tso_ctdna_tumor_only/402302087a640e1c/L4300185/Results/PRJ430058_L4300185/PRJ430058_L4300185.bam",
        ]

        mock_bogus_prefix = "ssssssssssssssssssssssssssssssssssssssssomalier_exxxxxtraaacccttt"

        for bp in mock_bam_paths:
            run_name = HolmesPipeline.get_step_function_instance_name(prefix=mock_bogus_prefix, index=bp)
            logger.info(run_name)
            self.assertTrue(len(run_name) <= max_length)
