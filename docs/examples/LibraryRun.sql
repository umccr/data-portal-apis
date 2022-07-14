-- Usage:
--  To be used in Portal Athena Query Editor
--  LibraryRun aka metadata link to a sequencing run

describe data_portal_libraryrun;
describe data_portal_labmetadata;
describe data_portal_sequencerun;
describe data_portal_sequence;


-- BSSH SequenceRun events
select * from data_portal_sequencerun where instrument_run_id = '220701_A01052_0106_BHGWFYDSX3';

-- Pipeline internal Sequence model
select * from data_portal_sequence where instrument_run_id = '220701_A01052_0106_BHGWFYDSX3';

-- key difference is that Sequence has only captured its final state at point-in-time i.e. mutation

-- we ingest LibraryRun upon first time BSSH event hitting to Pipeline
-- we extract them from SampleSheet.csv, then look up against LabMetadata
select * from data_portal_libraryrun where instrument_run_id = '220701_A01052_0106_BHGWFYDSX3';

-- so, we can make join call like this
select * from data_portal_labmetadata meta
    join data_portal_libraryrun lbr on lbr.library_id = meta.library_id
where lbr.instrument_run_id = '220701_A01052_0106_BHGWFYDSX3';

-- pls note that we strip topup and rerun suffix from library ID into LibraryRun
-- its discriminator uniqueness, therefore, is surrogate key of lane number + run info

select * from data_portal_labmetadata where library_id like '%L2200725%';
select * from data_portal_libraryrun where library_id like '%L2200725%';

-- NOTES:
-- justification is that generally the base library meta info should match up with topup
-- hence "topup" and/or "rerun", conceptually
-- this is a fix we made to Library meta info at its entry into the Pipeline world!
-- tricky bit is, run specific settings info such as Override Cycle
-- therefore we also deduce these in LibraryRun, along with QC status and coverage yield

