-- ### set these vars
set @instr_run_id='220121_A00130_0197_BHWJ2HDSX2';
set @cut_off_time='2022-02-04 23:59:14.249707';  -- do not touch data after this time, useful to preserve rerun cases


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
  and wfl.type_name != 'bcl_convert'
  and wfl.start < @cut_off_time
group by wfl.portal_run_id;


-- ### find all primary workflow output gds path related to this instrument run id
select concat_ws('/', 'gds://production', 'primary_data', lbr.instrument_run_id, wfl.portal_run_id, '') as gds,
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
  and wfl.type_name = 'bcl_convert'
  and wfl.start < @cut_off_time
group by wfl.portal_run_id;

-- ### find all workflows related to this instrument run id
select lbr.library_id, wfl.wfr_id, wfl.start, wfl.wfr_name, wfl.type_name, wfl.portal_run_id, wfl.end_status
from data_portal.data_portal_libraryrun lbr
    inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
    inner join data_portal.data_portal_workflow wfl on linker.workflow_id = wfl.id
where lbr.instrument_run_id = @instr_run_id
  and wfl.start < @cut_off_time
group by wfl.portal_run_id;

-- ### mark them failed
update data_portal.data_portal_libraryrun lbr
	inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id=lbr.id
    inner join data_portal.data_portal_workflow wfl on linker.workflow_id=wfl.id
set wfl.end_status='Failed'
where
	lbr.instrument_run_id=@instr_run_id
    and wfl.start < @cut_off_time;

-- Find FastqListRows from not Succeeded conversions
select * from data_portal.data_portal_fastqlistrow 
where rgid like concat('%', @instr_run_id, '%')
 and read_1 not regexp(concat('.+/', COALESCE((select distinct(wfl.portal_run_id)
		from data_portal.data_portal_libraryrun lbr
    		inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
    		inner join data_portal.data_portal_workflow wfl on linker.workflow_id = wfl.id
		where lbr.instrument_run_id = @instr_run_id
  		 and wfl.type_name = 'bcl_convert'
  		 and wfl.end_status = 'Succeeded'), 'XXXXX'), '/.+'));

--delete from data_portal.data_portal_fastqlistrow 
where rgid like concat('%', @instr_run_id, '%')
 and read_1 not regexp(concat('.+/', COALESCE((select distinct(wfl.portal_run_id)
		from data_portal.data_portal_libraryrun lbr
    		inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
    		inner join data_portal.data_portal_workflow wfl on linker.workflow_id = wfl.id
		where lbr.instrument_run_id = @instr_run_id
  		 and wfl.type_name = 'bcl_convert'
  		 and wfl.end_status = 'Succeeded'), 'XXXXX'), '/.+'));


-- ### ONLY NEEDED FOR THOSE CASE OF TOTALLY FAILED RUN. NOT NEEDED FOR RERUN CASES.

-- tables:
-- sequence
-- fastqlistrow
-- libraryrun <> libraryrun_workflows (need to de-associate links)
-- limsrow <> s3lim (<-- need to de-associate links if there exists related s3 entries)


-- ### mark sequence status failed
select * from data_portal.data_portal_sequence where instrument_run_id=@instr_run_id;
-- update data_portal.data_portal_sequence set status='failed' where instrument_run_id=@instr_run_id;


-- ### delete fastq list row entries
select * from data_portal.data_portal_fastqlistrow where rgid like concat('%', @instr_run_id, '%');
-- delete from data_portal.data_portal_fastqlistrow where rgid like concat('%', @instr_run_id, '%');


-- ### find libraryrun <> libraryrun_workflows <> workflow for this instrument run id
select lbr.id,
       lbr.lane,
       lbr.library_id,
       linker.id,
       linker.libraryrun_id,
       linker.workflow_id,
       wfl.id,
       wfl.type_name,
       wfl.end_status
from data_portal.data_portal_libraryrun lbr
    inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
    inner join data_portal.data_portal_workflow wfl on linker.workflow_id = wfl.id
where lbr.instrument_run_id = @instr_run_id;


-- ### delete libraryrun_workflows linking entries
-- delete linker
-- from data_portal.data_portal_libraryrun lbr
--     inner join data_portal.data_portal_libraryrun_workflows linker on linker.libraryrun_id = lbr.id
-- where lbr.instrument_run_id = @instr_run_id;


-- ### delete libraryrun entries
select * from data_portal.data_portal_libraryrun where instrument_run_id=@instr_run_id;
-- delete from data_portal.data_portal_libraryrun where instrument_run_id=@instr_run_id;


-- ### delete limsrow entries
select * from data_portal.data_portal_limsrow where illumina_id=@instr_run_id;
-- delete from data_portal.data_portal_limsrow where illumina_id=@instr_run_id;
