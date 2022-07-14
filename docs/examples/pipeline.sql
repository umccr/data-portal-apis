-- Usage:
--  To be used in Portal Athena Query Editor
--  fusion between SequenceRun, Workflow, LibraryRun, LabMetadata & mashup!

describe data_portal_sequencerun;
describe data_portal_workflow;
describe data_portal_libraryrun;
describe data_portal_libraryrun_workflows;
describe data_portal_labmetadata;


-- note, only lowercase type names are valid, uppercase types are deprecated
select distinct(type_name) from data_portal_workflow;

select wfl.id,
    wfl.wfr_name,
    wfl.type_name,
    wfl.wfr_id,
    wfl.wfv_id,
    wfl.version,
    wfl.start,
    "wfl"."end",
    wfl.end_status,
    sqr.*
from data_portal_workflow wfl
    join data_portal_sequencerun sqr on sqr.id = wfl.sequence_run_id
where
    sqr.instrument_run_id = '211125_A00130_0185_AHWC2HDSX2';

select wfl.id,
    wfl.wfr_name,
    wfl.type_name,
    wfl.wfr_id,
    wfl.wfv_id,
    wfl.version,
    wfl.start,
    "wfl"."end",
    wfl.end_status,
    sqr.*
from data_portal_workflow wfl
    join data_portal_sequencerun sqr on sqr.id = wfl.sequence_run_id
where
    -- wfl.type_name = 'bcl_convert' and
    wfl.end_status not in ('Deleted', 'Failed') and
    sqr.instrument_run_id = '211125_A00130_0185_AHWC2HDSX2';

--
--

select wfl.id,
    wfl.wfr_name,
    wfl.type_name,
    wfl.wfr_id,
    wfl.wfv_id,
    wfl.version,
    wfl.start,
    "wfl"."end",
    wfl.end_status,
    lbr.*,
    meta.*
from data_portal_workflow wfl
    join data_portal_libraryrun_workflows linker on linker.workflow_id = wfl.id
    join data_portal_libraryrun lbr on lbr.id = linker.libraryrun_id
    join data_portal_labmetadata meta on meta.library_id = lbr.library_id
where
    wfl.end_status not in ('Deleted', 'Failed') and
    lbr.instrument_run_id = '211125_A00130_0185_AHWC2HDSX2';
