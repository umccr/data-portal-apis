-- Usage:
--  To be used in Portal Athena Query Editor
--  FastqListRow table capture primary data conversion
--  Model holds very similar to BCL_Convert output FastqListRow with slight transformation done
--  Each record has a foreign key (FK) link to a particular SequenceRun ID with status is PendingAnalysis
--  SequenceRun models after BSSH event

-- check their shape
describe data_portal_fastqlistrow;
describe data_portal_sequencerun;
describe data_portal_limsrow;
describe data_portal_labmetadata;
describe data_portal_libraryrun;


-- get latest 5 runs from lims
select distinct(illumina_id) from data_portal_limsrow order by illumina_id desc limit 5;

-- get all sequencing info of this run
select * from data_portal_sequencerun where instrument_run_id = '220708_A00130_0218_BHGVWYDSX3' order by id desc;

-- get all FastqListRow from the run with -- status == PendingAnalysis
select * from data_portal_fastqlistrow where sequence_run_id = 1288;

-- great! can we make it in one statement?
select * from data_portal_fastqlistrow as fqlr
    join data_portal_sequencerun as sqr on sqr.id = fqlr.sequence_run_id
where
    sqr.instrument_run_id = '220708_A00130_0218_BHGVWYDSX3' and
    sqr.status = 'PendingAnalysis';

-- nice! but I don't want wide columns, just right side only
select fqlr.* from data_portal_fastqlistrow as fqlr
    join data_portal_sequencerun as sqr on sqr.id = fqlr.sequence_run_id
where
    sqr.instrument_run_id = '220708_A00130_0218_BHGVWYDSX3' and
    sqr.status = 'PendingAnalysis';

-- can we have a set of Libraries that only associated to this run?
select * from data_portal_libraryrun where instrument_run_id = '220708_A00130_0218_BHGVWYDSX3';

-- ok, but I need them as in (Lab) metadata tracking sheet?
select meta.* from data_portal_labmetadata meta
    join data_portal_libraryrun lbr on lbr.library_id = meta.library_id
where
    lbr.instrument_run_id = '220708_A00130_0218_BHGVWYDSX3';

-- right! so, if we join call on all these... yup, we got a big "wide column" table!! aka denormalized form
select * from data_portal_fastqlistrow fqlr
    join data_portal_sequencerun sqr on sqr.id = fqlr.sequence_run_id
    join data_portal_labmetadata meta on meta.library_id = fqlr.rglb
    join data_portal_libraryrun lbr on lbr.library_id = meta.library_id
where
    sqr.instrument_run_id in ('220708_A00130_0218_BHGVWYDSX3') and
    sqr.status = 'PendingAnalysis';

-- or, let just grab Bedoui Fastq from these sequencing...
select fqlr.read_1, fqlr.read_2 from data_portal_fastqlistrow fqlr
    join data_portal_sequencerun sqr on sqr.id = fqlr.sequence_run_id
    join data_portal_labmetadata meta on meta.library_id = fqlr.rglb
    join data_portal_libraryrun lbr on lbr.library_id = meta.library_id
where
    sqr.instrument_run_id in ('220708_A00130_0218_BHGVWYDSX3', '220701_A01052_0106_BHGWFYDSX3') and
    sqr.status = 'PendingAnalysis' and
    meta.project_owner = 'Bedoui';
