-- Get DRAGEN ctDNA QC metrics by Project aggregate
--
-- It is recommended to use full path for table FROM clause
-- e.g. AwsDataCatalog.multiqc.dragen_ctdna

select project_name, project_owner,
    round(avg(reads_tot_input_dragen)) as reads,
    round(avg(insert_len_mean_dragen)) as insert_size,
    round(avg(reads_num_dupmarked_pct_dragen)) as dup,
    round(avg(reads_qcfail_pct_dragen)) as fail,
    round(avg(reads_mapped_pct_dragen)) as mapped,
    round(avg(bases_q30_dragen)) as q30,
    round(avg(bases_q30_pct_dragen)) as q30_perc
from AwsDataCatalog.multiqc.dragen_ctdna
group by project_name, project_owner;
