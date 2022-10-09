-- !preview conn=con

select
  meta.subject_id,
  meta.sample_id,
  meta.project_name,
  meta.phenotype,
  meta.source,
  meta."type",
  meta.workflow,
  qc.insert_len_mean_dragen,
  qc.insert_len_std_dev_dragen,
  qc.tmb_status_purple,
  qc.tmb_sv_purple,
  qc.indel_umccrise,
  qc.snp_umccrise
from 
  AwsDataCatalog.multiqc.dragen_umccrise as qc
  inner join data_portal.data_portal.data_portal_limsrow as meta on meta.sample_id = qc.sample_id
where
  meta.phenotype = 'tumor' and meta.project_name = 'CUP' and year = '2022'
order by meta.subject_id desc;
