from data_extractor_dao import executeQuery
import db_queries as dq


queries = ('''
update idw.team_dim set is_active=false
where team_name not like '%Artex%';

update idw.planview_projects_dim
set pv_aha_id=200
where pv_project_name='Periop Surgical History (Dev Partner)' and pv_aha_id is null;

update idw.planview_projects_dim
set pv_aha_id=216
where pv_project_name='SOC2 Compliance' and pv_aha_id is null;

update idw.planview_projects_dim
set pv_aha_id=1
where pv_project_name='IPL Editor Tool' and pv_aha_id is null;

update  idw.team_dim
set jira_rapid_view_id=83
where team_name='Artex';

insert into idw.holiday_dim (holiday_date, holiday_name, Holiday_country, holiday_state,is_mandatory_holiday)
select '2019-12-24 00:00:00'::timestamp,'Day Before Christmas Holiday','US','US',true 
where  not exists (select 'x' from idw.holiday_dim where holiday_date='2019-12-24 00:00:00'::timestamp) union
select '2019-12-25 00:00:00'::timestamp,'Christmas','US','US',true 
where  not exists (select 'x' from idw.holiday_dim where holiday_date='2019-12-25 00:00:00'::timestamp) union
select '2019-12-31 00:00:00'::timestamp,'New Years Eve','US','US',true 
where  not exists (select 'x' from idw.holiday_dim where holiday_date='2019-12-31 00:00:00'::timestamp) union
select '2020-01-01 00:00:00'::timestamp,'New Years Day','US','US',true 
where  not exists (select 'x' from idw.holiday_dim where holiday_date='2020-01-01 00:00:00'::timestamp);
''')


def executeBasicQueries():
    executeQuery(queries)
    executeQuery(dq.update_is_maintenance_flag_in_planview_project)
    executeQuery(dq.update_is_time_reportable_flag)
    executeQuery(dq.map_portfolio_and_project_dim_id_for_sprint_issues)
    executeQuery(dq.update_is_consultant_flag)
    executeQuery(dq.remove_system_user_from_team_map)
