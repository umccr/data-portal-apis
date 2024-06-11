-- ### Clean up script for a specific workflow run steps

-- ### set these vars
set @instr_run_id='220121_A00130_0197_BHWJ2HDSX2';
set @cut_off_time='2022-02-15 23:59:59';  -- do not touch data after this time, useful to preserve rerun cases
set @workflow_step='wts_tumor_only';

-- ### find all analysis workflow output gds path related to this instrument run id
select concat_ws('/', 'gds://production', 'analysis_data', substring(wfl.wfr_name, position('SBJ' in wfl.wfr_name), 8), wfl.type_name, wfl.portal_run_id, '') as gds,
       lbr.library_id,
       wfl.wfr_id,
       wfl.start,
       wfl.wfr_name,
       wfl.type_name,
       wfl.portal_run_id,
       wfl.end_status
from data_portal.data_portal_libraryrun lbr
    inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
    inner join data_portal.data_portal_workflow wfl on linker.workflow_id = wfl.id
where lbr.instrument_run_id = @instr_run_id
  and wfl.type_name = @workflow_step
  and wfl.start < @cut_off_time
group by wfl.portal_run_id;

-- ### mark them deleted
update data_portal.data_portal_libraryrun lbr
	inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id=lbr.id
    inner join data_portal.data_portal_workflow wfl on linker.workflow_id=wfl.id
set wfl.end_status='Deleted'
where
	lbr.instrument_run_id=@instr_run_id
    and wfl.type_name = @workflow_step
    and wfl.start < @cut_off_time;
