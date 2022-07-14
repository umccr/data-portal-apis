-- Usage:
--  1. select or highlight each line that ends with semicolon
--  2. click Run button
--
-- NOTE:
--  Please do not hit Run button without line being selected. It will run all statements!
--  To be used in Portal Athena Query Editor

show databases;

show tables;

-- portal dummy table
select * from data_portal_configuration;

-- insert into will fail, not supported in athena federated query
insert into data_portal_configuration (name, value) values ('insert', 'test');

-- delete will fail, not supported as well
delete from data_portal_workflow where id = 1;

-- show schema of workflow table
describe data_portal_workflow;

-- give me latest 10 sequencing info
select * from data_portal_workflow order by id desc limit 10;

-- let get workflow by id on some specific columns of interest
select id, wfr_name, type_name, wfr_id, end_status, portal_run_id from data_portal_workflow where id=4158;

-- using cast() function to transform timestamp type to varchar string
select id, wfr_name, type_name, wfr_id, end_status, portal_run_id, cast(start as varchar) as start from data_portal_workflow where id=4158;

-- 'END' is reserved keyword in Athena. Need to escape with double quotes
select id, wfr_name, type_name, wfr_id, end_status, portal_run_id, start, "end" from data_portal_workflow where id=4158;

--
-- Congrats!
-- Feel free to browse in "Saved queries" tab for more.
--
