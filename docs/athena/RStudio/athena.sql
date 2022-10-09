-- !preview conn=con

select 
  subject_id,
  sample_id,
  project_name,
  phenotype,
  insert_len_mean_dragen,
  insert_len_std_dev_dragen,
  tmb_status_purple,
  tmb_sv_purple,
  indel_umccrise,
  snp_umccrise
from 
  AwsDataCatalog.multiqc.dragen_umccrise
where
  phenotype = 'tumor' and project_name = 'CUP'
order by subject_id desc;
