-- Usage:
--  To be used in Portal Athena Query Editor
--  WGS Tumor Normal Fastq Pairing

describe data_portal_labmetadata;
describe data_portal_libraryrun;
describe data_portal_fastqlistrow;


-- grab WGS Subjects in a Run on Clinical or Research grade samples
select distinct(subject_id) from data_portal_labmetadata meta
    join data_portal_libraryrun lbr on lbr.library_id = meta.library_id
where
    lbr.instrument_run_id = '210923_A00130_0176_BHH5JFDSX2' and
    meta.workflow in ('clinical', 'research') and
    meta.type in ('WGS') and
    meta.phenotype in ('tumor', 'normal')
order by subject_id;

-- checkpoint
-- we should do QC, coverage and yield check among these subjects at this point
-- these should also be captured as part of LibraryRun (still pending work)

-- before going next with manually pairing them below
-- you can payload these Subjects into POST "/pairing" endpoint for auto-pairing
-- as follows
-- awscurl -X POST -d '["SBJ00951", "SBJ00987", "SBJ00994", "SBJ01004", "SBJ01005", "SBJ01007", "SBJ01008", "SBJ01009", "SBJ01010"]' -H "Content-Type: application/json" --profile prodops --region ap-southeast-2 "https://api.portal.prod.umccr.org/iam/pairing/by_subjects" | jq > pairing__176.json


-- subject normal libraries includes 'topup' and 'rerun' suffix
select * from data_portal_labmetadata
where
    subject_id = 'SBJ02405' and
    phenotype = 'normal' and
    type = 'WGS' and
    workflow = 'clinical';

-- subject tumor libraries
select * from data_portal_labmetadata
where
    subject_id = 'SBJ02405' and
    phenotype = 'tumor' and
    type = 'WGS' and
    workflow = 'clinical';

-- pool them together
select * from data_portal_labmetadata
where
    subject_id = 'SBJ02405' and
    type = 'WGS' and
    workflow = 'clinical';

-- so, there is topup for normal
-- for FastqListRow's rglb, we normalised this without _topup suffix
-- hence, you can look up with just 'base' library ID
select * from data_portal_fastqlistrow where rglb = 'L2200725';

-- note that 'rgid' or 'read_1' column the base library ID and topup one
-- were running pass different sequencing 105 and 106, respectively
-- therefore, base library ID for normal is L2200725 and tumor is L2200726

-- hence, here are tumor normal Fastq for SBJ02405
select * from data_portal_fastqlistrow where rglb in ('L2200725', 'L2200726');

-- but, can we have it all-in-one-go? yup, make a join call them!
select * from data_portal_labmetadata meta
    join data_portal_fastqlistrow fqlr on fqlr.rglb = meta.library_id
where
    meta.subject_id = 'SBJ02405' and
    meta.type = 'WGS' and
    meta.workflow = 'clinical';

-- or, better yet!
select fqlr.rgid, fqlr.rgsm, fqlr.rglb, fqlr.lane, fqlr.read_1, fqlr.read_2 from data_portal_labmetadata meta
    join data_portal_fastqlistrow fqlr on fqlr.rglb = meta.library_id
where
    meta.subject_id = 'SBJ02405' and
    meta.type = 'WGS' and
    meta.workflow = 'clinical';
