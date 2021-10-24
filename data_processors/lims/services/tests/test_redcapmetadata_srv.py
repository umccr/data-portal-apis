from data_portal.tests import factories
from data_portal.tests.factories import TestConstant
from data_processors.lims.services import redcapmetadata_srv
from data_processors.lims.tests.case import logger, LimsUnitTestCase
from data_portal.models import LabMetadata
from mockito import when, mock, kwargs, patch, ANY
import pandas as pd
import tempfile


mock_sheet = b"""recordid,redcap_event_name,redcap_repeat_instrument,redcap_repeat_instance,redcap_data_access_group,redcap_survey_identifier,enrolment_timestamp,enr_email,study_category,study_name,study_column_sub,study_name_research,study_name_research_other,study_subcategory,study_name_derived,subjectid,man_sbj,enr_patient_studyid,enr_patient_urn_local,enr_patient_urn_other,enr_patient_enrol_date,enr_patient_firstname,enr_patient_surname,enr_patient_dob,enr_patient_age,enr_patient_sex,enr_patient_ethnicity,enr_patient_race,enr_patient_address_line1,enr_patient_address_line2,enr_patient_address_suburb,enr_patient_address_state,enr_patient_address_postcode,enr_patient_phone,enr_patient_email,enr_patient_consent_column,enr_patient_consent_other,enr_patient_consent_date_other,enr_linked_subject,enr_linked_id_valid,enr_linked_recordid,enr_linked_subject2,enr_linked_id_valid2,enr_linked_recordid2,enr_comments,enr_create_user,enr_create_dt,createdate,createyear,createyear2,yearprefix,enrolment_complete,request_timestamp,req_request_date,req_requester_name,req_requester_email,req_requester_inst,req_requester_inst_other,req_clin_provider,req_clinprov_name,req_clinprov_email,req_pi,req_pi_name,req_pi_email,req_pi_institution,req_pi_institution_other,req_project,req_custodian_name,req_custodian_email,req_tumour_stream,req_tuscat_brn,req_tuscat_brt,req_tuscat_lgi,req_tuscat_ugi,req_tuscat_gu,req_tuscat_gyn,req_tuscat_hem,req_tuscat_hn,req_tuscat_lng,req_tuscat_ped,req_tuscat_sar,req_tuscat_skn,req_tuscat_pedspec,req_tuscat_oth,req_tuscat_calc,req_diagnosis,req_seq_type___1,req_seq_type___2,req_seq_type___3,req_seq_type___4,req_seq_type___99,req_seq_type_other,req_seq_reason___1,req_seq_reason___2,req_seq_reason___3,req_seq_reason___99,req_seq_reason_other,req_return_fastq,req_return_fastq_name,req_return_fastq_email,req_return_keybaseid,req_copy_clinician,req_copy_clinician1_name,req_copy_clinician1_email,req_copy_clinician2_name,req_copy_clinician2_email,req_comments,req_create_user,req_create_dt,request_complete,app_pi_name,app_pi_name_other,app_pi_email_other,app_pi_approved,app_approval_date,app_comments,app_create_user,app_create_dt,approval_complete,consent_patient_timestamp,cons_completedby,cons_2,cons_3,cons_4,cons_rel_name,cons_rel_relation,cons_rel_dob,cosn_rel_address,cons_rel_phone,cons_6,cons_7,cons_8,cons_9,cons_participant_name,cons_participant_signature,cons_participant_sig_date,cons_scanned_form,cons_witness_name,cons_witness_signature,cons_witness_sig_date,cons_consent_date,cons_create_user,cons_create_dt,consent_patient_complete,consent_pi_timestamp,conspi_pi_name,conspi_pi_signature,conspi_pi_sig_date,conspi_merged_file,conspi_create_user,conspi_create_dt,consent_pi_complete,clinical_phenotype_timestamp,clin_ecog_qsorres,clin_rsorres,clin_primtumloc,clin_ctx,clin_ctx_type,clin_ctx_num,clin_ctx_prior,clin_rtx,clin_genetic_prior,clin_genetic_testname,clin_genetic_germline,clin_genetic_germlinename,clin_genetic_somatic,clin_genetic_somaticname,clin_seq_concur,clin_seq_concurname,clin_history,clin_clinfile1,clin_clinfile2,clin_histopathfile1,clin_histopathfile2,clin_comments,clin_create_user,clin_create_dt,clinical_phenotype_complete,biopsy_sample_type_timestamp,sub_subtiming,sub_subtype___1,sub_subtype___2,sub_archival_fresh,sub_sample_type___1,sub_sample_type___2,sub_sample_type___3,sub_sample_type___4,sub_sample_type___5,sub_sample_type___6,sub_sample_type___7,sub_sample_type___8,sub_sample_type___99,sub_sample_type_other,sub_ffpe_custodiallab,sub_ffpe_identifier,sub_ffpe_recall,sub_biopsy_date,sub_biopsy_time,sub_biopsy_site,sub_biopsy_type,sub_biopsy_type_other,sub_blood_date,sub_germlinesample_type___1,sub_germlinesample_type___2,sub_germlinesample_type___3,sub_germlinesample_type___4,sub_germlinesample_type___5,sub_germlinesample_type___99,sub_germlinesample_type_other,sub_comments,sub_create_user,sub_create_dt,biopsy_sample_type_complete,tdna_id,tdna_id_external,tdna_starlims,tdna_starlimsid,tdna_mdxprj,tdna_mdxid,tdna_prjid,tdna_sample_source,sub_source_number,tdna_receivedate,tdna_idmatch,tdna_identifiers_diff,tdna_extraction,tdna_extractiondate,tdna_photo,tdna_photo2,tdna_submissionform,tdna_submissionform2,tdna_qcdate,tdna_nanodrop,tdna_dilfactor,tdna_qubit,tdna_stockconc,tdna_integrity,tdna_gapdh,tdna_passqc,tdna_qcwhyfail,tdna_qcwhyfail_other,tdna_resub_email,tdna_cfdna,tdna_cfdna_total,tdna_libprepdate,tdna_batchid,tdna_libraryid,tdna_libprep,tdna_libprep_whyfail,tdna_libprep_whyfail_other,tdna_seqdate,tdna_seq_success,tdna_seq_whyfail,tdna_seq_whyfail_other,tdna_topup,tdna_topupdate,tdna_topup_libraryid,tdna_samplereturned,tdna_samplereturn_date,tdna_comments,tdna_create_user,tdna_create_dt,lab_tumour_dna_complete,trna_id,trna_id_external,trna_starlims,trna_starlimsid,trna_mdxprj,trna_mdxid,trna_prjid,trna_sample_source,sub_source_number_278db1,trna_receivedate,trna_idmatch,trna_identifiers_diff,trna_extraction,trna_extractiondate,trna_photo,trna_photo2,trna_submissionform,trna_submissionform2,trna_qcdate,trna_nanodrop,trna_dilfactor,trna_qubit,trna_stockconc,trna_integrity,trna_gapdh,trna_passqc,trna_qcwhyfail,trna_qcwhyfail_other,trna_resub_email,trna_libprepdate,trna_batchid,trna_libraryid,trna_libprep,trna_libprep_whyfail,trna_libprep_whyfail_other,trna_seqdate,trna_seq_success,trna_seq_whyfail,trna_seq_whyfail_other,trna_topup,trna_topupdate,trna_topup_libraryid,trna_samplereturned,trna_samplereturn_date,trna_comments,trna_create_user,trna_create_dt,lab_tumour_rna_complete,gdna_id,gdna_id_external,gdna_starlims,gdna_starlimsid,gdna_mdxprj,gdna_mdxid,gdna_prjid,gdna_sample_source,sub_source_number_2c4175,gdna_receivedate,gdna_idmatch,gdna_identifiers_diff,gdna_extraction,gdna_extractiondate,gdna_photo,gdna_photo2,gdna_submissionform,gdna_submissionform2,gdna_qcdate,gdna_nanodrop,gdna_dilfactor,gdna_qubit,gdna_stockconc,gdna_integrity,gdna_gapdh,gdna_passqc,gdna_qcwhyfail,gdna_qcwhyfail_other,gdna_resub_email,gdna_libprepdate,gdna_batchid,gdna_libraryid,gdna_libprep,gdna_libprep_whyfail,gdna_libprep_whyfail_other,gdna_seqdate,gdna_seq_success,gdna_seq_whyfail,gdna_seq_whyfail_other,gdna_topup,gdna_topupdate,gdna_topup_libraryid,gdna_samplereturned,gdna_samplereturn_date,gdna_comments,gdna_create_user,gdna_create_dt,lab_germline_dna_complete,rnasum_ref,snomed_code,seq_fail,seq_failreason,seq_fail_reason_other,seq_fail_report,seq_fail_report_other,seq_fail_email,seq_coverage_tumour,seq_coverage_normal,sample_quality___1,sample_quality___2,sample_quality___3,sample_quality___4,ploidy,tumour_purity,polyclonal_proportion,mutations_mbase,coding_variants,snv_number,indels_number,variants_tier1,variants_tier2,variants_tier3,variants_tier4,pathogen,circosplot,genome_integrity,mutator_type,signature,msi_status,tissueorigin,md_germlinecurr,md_germlinecurrname,md_somaticcurr,md_somaticcurrname,report_yesno,report_date,report_file,mol_create_user,mol_create_dt,molecular_data_reporting_complete,clinical_follow_up_timestamp,fup_eventdate,fup_vs,dthdat_dsyn,dthdat,alivedat,newtxyn,cmtrt1,cmdur1,cm1_rsorres,cmtrt2,cmdur2,cm2_rsorres,cmtrt3,cmdur3,cm3_rsorres,cmtrt4,cmdur4,cm4_rsorres,cmtrt5,cmdur5,cm5_rsorres,mngimpt,cm_mngimpt___1,cm_mngimpt___2,cm_mngimpt___3,cm_mngimpt___4,cm_mngimpt___5,trial_mngimpt,trialphase_mngimpct,trialid_mngimpct,capyn,cm_cap___1,cm_cap___2,cm_cap___3,cm_cap___4,cm_cap___5,useful_mngimpct,detail_mngimpct___1,detail_mngimpct___2,detail_mngimpct___3,detail_mngimpct___4,detail_mngimpct___5,detail_mngimpct___6,detail_mngimpct___7,detail_mngimpct___99,detailoth_mngimpct,fup_fccref,fup_create_user,fup_create_dt,clinical_follow_up_complete,subject_status_summary_complete,withdrawal_patient_timestamp,withdrawal_completed_by,participant_name_v2,participant_signature_v2,participant_signature_date_v2,participant_withdrawal_scanned,wdr_create_user,wdr_create_dt,withdrawal_patient_complete,withdrawal_pi_timestamp,withdrawal_pi_name,withdrawal_pi_signature,withdrawal_pi_signature_date,withdrawal_final,wdrp_create_user,wdrp_create_dt,withdrawal_pi_complete,fhirtest,todo_dev_complete
5001,enrolment_arm_1,,,_stafford_fox,,,,1,6,,,,3,"Stafford Fox",SBJ05001,,SFRC01442,MEH745025,SVP745025,2021-09-09,,,,,1,7,7,,,,,,,,,1,2021-08-03,,1,0,,0,0,"This is a deidentified sample",grant.lee,"2021-09-12 14:43:41",2021-09-12,21,21,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
5001,request_original_arm_1,,,_stafford_fox,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2021-09-10,"Ratana Lim",umccr-redcap@unimelb.edu.au,9,,0,"Briony Milesi",umccr-redcap@unimelb.edu.au,,,,,,Scott-SFRC,"Clare Scott",precision-oncology@unimelb.edu.au,8,,,,,,,,99,,,,,,Tonsil,Tonsil,"80 yo male with tonsillar squamous cell carcinoma (left side), T2N2bM0, Dx (by review) August 2021. p16 postitive. Infrequent smoker (less than one pack-year smoking history). Treated with curative intent chemoradiotherapy (with cisplatin) starting 0/08/21. Submitted sample is surgery tissue obtained 30-07-2021",0,1,0,0,0,,0,1,0,0,"Required for patient treatment  ",1,"Grant Jones",umccr-redcap@unimelb.edu.au,GJ7766,1,"Clare Scott",umccr-redcap@unimelb.edu.au,"Damien Kee",umccr-redcap@unimelb.edu.au,"No additional comments.",grant.lee,"2021-09-12 14:50:11",2,1,,,1,2021-09-11,"I approved this request, and these are my comments.",grant.lee,"2021-09-12 14:52:55",2,,,,,,,,,,,,,,,,,,,,,,,,,0,,,,,,,,0,,,2,"Left tonsil",1,Cisplatin,0,,1,0,,,,,,0,,"80 yo male with tonsillar squamous cell carcinoma (left side), T2N2bM0, Dx (by review) August 2021. p16 postitive. Infrequent smoker (less than one pack-year smoking history). Treated with curative intent chemoradiotherapy (with cisplatin) starting 30/08/21. Submitted sample is surgery tissue obtained 30-07-2021  ",REDCap-Beginners-Guide.pdf,,REDCap-Beginners-Guide.pdf,,"No additional comments.",grant.lee,"2021-09-12 14:55:31",2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,0,0,0,0,0,,,,,0,0,0,0,0,,0,0,0,0,0,0,0,0,,,,,0,,,,,,,,,,,,,,,,,,,,"""

class RedcapMetadataSrvUnitTests(LimsUnitTestCase):
    def test_get_metadata(self):
        """
        python manage.py test data_processors.lims.services.tests.test_redcapmetadata_srv.RedcapMetadataSrvUnitTests.test_get_metadata
        """
        make_mock_labmeta()
        mock_csv = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)
        mock_csv.write(mock_sheet.lstrip().rstrip())
        mock_csv.seek(0)
        mock_csv.flush()
        when(redcapmetadata_srv).download_redcap_project_data(ANY).thenReturn(pd.read_csv(mock_csv))

        meta = redcapmetadata_srv.retrieve_metadata({"subjectid":["SBJ05001"]},None)
        self.assertIsNotNone(meta)

    def test_get_metadata_not_found(self):
        """
        python manage.py test data_processors.lims.services.tests.test_redcapmetadata_srv.RedcapMetadataSrvUnitTests.test_get_metadata_not_found
        """

        make_mock_labmeta()
        mock_csv = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)
        mock_csv.write(mock_sheet.lstrip().rstrip())
        mock_csv.seek(0)
        mock_csv.flush()
        when(redcapmetadata_srv).download_redcap_project_data(ANY).thenReturn(pd.read_csv(mock_sheet))

        meta = redcapmetadata_srv.retrieve_metadata({"subjectid":["XXXFOOBAR"]},None)
        self.assertIsNone(meta)


def make_mock_labmeta():
        lab_meta = LabMetadata(
            library_id="L2101080",
            sample_name="SAMIDA-EXTSAMA",
            sample_id="SAMIDA",
            external_sample_id="EXTSAMA",
            subject_id="SBJ05006",
            external_subject_id="EXTSUBID_A",
            phenotype="NORMAL",
            quality="good",
            source="FFPE",
            project_name="Foo",
            project_owner="Roo",
            experiment_id="Exper1",
            type="WTS",
            assay="NebRNA",
            override_cycles="Y151;I8;I8;Y151",
            workflow="clinical",
            coverage="6.0",
            truseqindex="H07"
        )
        lab_meta.save()