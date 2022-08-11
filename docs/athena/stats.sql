-- stats

-- shape
describe data_portal.data_portal_workflow;

-- size
select count(1) as total_wfl_runs from data_portal.data_portal_workflow;

-- all possible distinct workflow runs types
select distinct(wfl.type_name) from data_portal.data_portal_workflow as wfl;

-- all possible distinct workflow runs end statuses
select distinct(wfl.end_status) from data_portal.data_portal_workflow as wfl;

-- total bcl_convert runs
select count(1) as total_bcl_convert_wfl_runs from data_portal.data_portal_workflow as wfl where wfl.type_name in ('bcl_convert', 'BCL_CONVERT');

-- extract all workflow runs by bcl conversion, sorted descending
select
    wfl.id,
    wfl.wfr_name,
    wfl.sample_name,
    wfl.type_name,
    wfl.wfr_id,
    wfl.wfl_id,
    wfl.wfv_id,
    wfl.version,
    -- wfl.input,
    wfl.start,
    -- wfl.output,
    "wfl"."end",
    wfl.end_status,
    wfl.notified,
    wfl.sequence_run_id,
    wfl.batch_run_id,
    wfl.portal_run_id
from
    data_portal.data_portal_workflow as wfl
where
    wfl.type_name in ('bcl_convert', 'BCL_CONVERT')
    -- and wfl.end_status in ('Succeeded', 'Failed', 'Aborted', 'Started', 'Deleted', 'Deleted;;issue475')
order by id desc
-- limit 10
;
