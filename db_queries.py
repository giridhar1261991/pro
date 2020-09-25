

get_sprints_to_update_metrics = (
    '''select distinct sprint_number, team_dim_id from idw.jira_sprint_summary where coalesce(is_metrics_updated,false)=false;''')

update_estimate_sprint_points = ('''update idw.jira_sprint_summary ss
set estimated_points=t.estimated_points
from (
    select s.sprint_number, s.team_dim_id,
    sum((ticket_details -> 'fields' ->> 'customfield_10411')::numeric) as estimated_points
    from idw.sprint_issues s
    where s.is_active=true 
    and sprint_number in (select sprint_number from idw.jira_sprint_summary 
    where coalesce(is_metrics_updated,false)=false)
    group by s.sprint_number, s.team_dim_id
) as t
where ss.sprint_number=t.sprint_number and ss.team_dim_id=t.team_dim_id''')

update_actual_completed_sprint_points = ('''update idw.jira_sprint_summary s1
            set actual_completed_points = t.completed_points
            from
            (
                select s.sprint_number, s.team_dim_id,
                sum((ticket_details -> 'fields' ->> 'customfield_10411')::numeric) as completed_points
                from idw.sprint_issues s inner join idw.jira_sprint_summary ss
                on ss.sprint_number=s.sprint_number and ss.team_dim_id=s.team_dim_id and
                coalesce((ticket_details -> 'fields' ->> 'resolutiondate')::timestamp, now()) between ss.sprint_start_date and ss.sprint_complete_date
                where coalesce(is_metrics_updated,false)=false and s.is_active=true
                and ticket_details -> 'fields' ->> 'resolutiondate' is not null
                group by s.sprint_number, s.team_dim_id
            ) t where t.sprint_number=s1.sprint_number and t.team_dim_id=s1.team_dim_id;''')

update_completed_sprint_points = ('''update idw.jira_sprint_summary s1
            set completed_points = t.completed_points
            from
            (
                select s.sprint_number, s.team_dim_id,
                sum((ticket_details -> 'fields' ->> 'customfield_10411')::numeric) as completed_points
                from idw.sprint_issues s inner join idw.jira_sprint_summary ss
                on ss.sprint_number=s.sprint_number and ss.team_dim_id=s.team_dim_id 
                where  coalesce(is_metrics_updated,false)=false and s.is_active=true and
                (coalesce((ticket_details -> 'fields' ->> 'resolutiondate')::timestamp, now()) >= ss.sprint_start_date
				and coalesce(status_on_sprint_closure,'Open')!='Open')
                group by s.sprint_number, s.team_dim_id
            ) t where t.sprint_number=s1.sprint_number and t.team_dim_id=s1.team_dim_id;''')

update_team_members = ('''update idw.jira_sprint_summary ss
set number_of_devs=t.no_of_devs
from     (
        select u.team_dim_id,ss.sprint_number, count(u.user_dim_id) as no_of_devs  
        from idw.jira_sprint_summary ss
        inner join idw.user_team_map u on ss.team_dim_id=u.team_dim_id
        and u.team_entry_date < ss.sprint_start_date and coalesce(u.team_exit_date,now()) > ss.sprint_complete_date
        where coalesce(is_metrics_updated,false)=false
        group by u.team_dim_id,ss.sprint_number
        order by u.team_dim_id,ss.sprint_number
    ) t
    where ss.sprint_number=t.sprint_number and ss.team_dim_id=t.team_dim_id;''')
 
update_actual_sprint_date = ('''update idw.jira_sprint_summary s
set actual_sprint_start_date=actual_start_date::date
from (select sprint_number,sprint_start_date +case when (LAG(sprint_complete_date,1)
								over (order by team_dim_id,sprint_number,sprint_start_Date))::date = jira_sprint_summary.sprint_start_date::date
												then '1 day'::interval else '0 day'::interval end as actual_start_date
		from idw.jira_sprint_summary) s1	
where s.sprint_number=s1.sprint_number
and actual_sprint_start_date is null''')

update_sprint_days = ('''
update idw.jira_sprint_summary
set total_sprint_days=idw.weekdays_sql(actual_sprint_start_date,sprint_complete_date),
actual_sprint_days=idw.weekdays_holiday_sql(actual_sprint_start_date,sprint_complete_date)
where coalesce(is_metrics_updated,false)=false;''')

update_maintenance_flag = ('''update idw.jira_sprint_summary
set is_metrics_updated=true
where coalesce(is_metrics_updated,false)=false;''')

update_is_maintenance_flag_in_planview_project = ('''
update idw.planview_projects_dim
set is_maintenance_project=CASE when lower(pv_project_name) like '%(ongoing)%' then true else false end
where is_maintenance_project is null or is_maintenance_project=false;''')

update_is_time_reportable_flag = ('''update idw.planview_project_tasks pp
set is_time_reportable=false
from idw.planview_projects_dim ppd 
where ppd.pv_project_dim_id=pp.pv_project_dim_id
and ppd.category = 'Services' and is_time_reportable=true;''')

map_portfolio_and_project_dim_id_for_sprint_issues = ('''update idw.sprint_issues i
set portfolio_dim_id=pp.portfolio_dim_id,
pv_project_dim_id=pp.pv_project_dim_id,
is_maintenance=false
from  idw.epic_dim e
inner join (select pp.pv_project_name,pp.pv_project_dim_id,pp.pv_aha_id,pd.portfolio_dim_id,pd.portfolio_name
		   from idw.planview_projects_dim pp left join idw.portfolio_dim pd on pp.portfolio_dim_id=pd.portfolio_dim_id) pp
on (trim(reverse(split_part(reverse(e.aha_reference), '/', 1))) = pp.pv_aha_id or  e.aha_reference = pp.pv_aha_id)
where i.epic_dim_id=e.epic_dim_id and i.is_active=true and i.portfolio_dim_id is null;

update idw.sprint_issues i 
set portfolio_dim_id=pp.portfolio_dim_id,
pv_project_dim_id=pp.pv_project_dim_id,
is_maintenance=true
from  idw.jira_project_dim jp inner join 
(select pp.portfolio_dim_id,pp.portfolio_name,ppd.pv_project_dim_id from idw.planview_projects_dim ppd left join idw.portfolio_dim pp on
 pp.portfolio_dim_id=ppd.portfolio_dim_id
where lower(ppd.category) != 'services' and is_maintenance_project=true) pp
 on lower(replace(trim(jp.project_name),' ','')) like '%' || lower(replace(trim(pp.portfolio_name),' ','%')) || '%'
where i.portfolio_dim_id is null and i.project_dim_id=jp.project_dim_id and i.is_active=true;''')

timesheet_population_query = ('''select 
    0 as companyId,
    1 as entryTypeId,
    dist_ts.timesheetid as timesheetId, 
    dwts.resource_ppm_id as userId, 
    dwts.project_ppm_id as level1Id,
    dwts.task_ppm_id as level2Id,
    to_char(dwts.date_of_sprint, 'YYYY/MM/DD') as entryDate,
    coalesce(pvts.entryId, 0) as entryId,
    dwts.resource_role_id as level3Id,
    dwts.per_day_time_spent as entryHours
    from idw.planview_get_timesheet_entries('{0}')  as dwts 
    inner join (select distinct timesheetid, userId, startDate,endDate from timesheet_data ) dist_ts
    on  dwts.resource_ppm_id = dist_ts.userId 
    and dwts.date_of_sprint::date between dist_ts.startDate::date and dist_ts.endDate::date
    left join timesheet_data as pvts
    on  dwts.resource_ppm_id = pvts.userId
    and dwts.project_ppm_id=pvts.project_ppm_id
    and dwts.task_ppm_id=pvts.task_id
    and dwts.date_of_sprint::date = pvts.entrydate::date 
    and pvts.userId=dist_ts.userId and pvts.timesheetid=dist_ts.timesheetid
    and dwts.date_of_sprint::date between pvts.startDate::date and pvts.endDate::date;''')

get_sprints_for_timesheet = ('''
    SELECT string_agg(sprint_number::varchar(15), ', ') AS sprint_number 
    from (select distinct sprint_number 
    FROM idw.jira_sprint_summary a 
    where coalesce(a.is_timesheet_populated,false)=false 
	and coalesce(a.is_metrics_updated,false)=true 
    and a.sprint_end_date > '2020-05-14' 
    and a.sprint_end_date <= current_date)x1;''')

update_is_consultant_flag = ('''update idw.planview_resource_dim
set is_consultant=case when lower(type) like '%contractor%' or lower(type) like '%consultant%' then true else false end
where is_consultant is null;''')

get_userlist_to_populate_timesheet = ('''select string_agg(a1.resource_ppm_id::varchar(15), ', ') AS user_list, to_char(min(a1.sprint_start_date), 'YYYY/MM/DD') as min 
            from ( SELECT d.resource_ppm_id, min(a.sprint_start_date) as sprint_start_date 
            FROM idw.jira_sprint_summary a 
            inner join idw.user_team_map c ON a.team_dim_id = c.team_dim_id 
            inner join idw.planview_resource_dim d ON c.user_dim_id = d.resource_dim_id AND c.team_entry_date <= a.sprint_start_date 
            AND COALESCE(c.team_exit_date::timestamp with time zone, now()) >= a.sprint_end_date 
            where coalesce(a.is_timesheet_populated,false)=false and coalesce(a.is_metrics_updated,false)=true and a.sprint_end_date > '2020-05-14' 
            and a.sprint_end_date <= current_date group by d.resource_ppm_id 
            ) a1;''')

update_timesheet_dim_ids = ('''update idw.planview_timesheet_entries pt
set resource_dim_id = r.resource_dim_id
from idw.planview_resource_dim r
where pt.userid=r.resource_ppm_id and pt.resource_dim_id is null;

update idw.planview_timesheet_entries pt
set team_dim_id = t.team_dim_id
from idw.user_team_map t
where pt.resource_dim_id = t.user_dim_id 
and pt.entryDate 
between t.team_entry_date::date and coalesce(t.team_exit_date,now())::date
and pt.team_dim_id is null;

update idw.planview_timesheet_entries pt
set project_dim_id = p.pv_project_dim_id,
portfolio_dim_id = p.portfolio_dim_id
from idw.planview_projects_dim p
where pt.entryTypeid = 1 and pt.level1id = p.project_ppm_id::bigint 
and (pt.project_dim_id is null or pt.portfolio_dim_id is null);

update idw.planview_timesheet_entries pt
set task_dim_id = t.task_dim_id
from idw.planview_project_tasks t
where pt.entryTypeid = 1 and pt.level2id = t.task_ppm_id::bigint
and pt.task_dim_id is null;

update idw.planview_timesheet_entries pt
set portfolio_dim_id = p.portfolio_dim_id
from idw.portfolio_dim p
where pt.entryTypeid = 99 and pt.level1id = p.portfolio_ppm_id::bigint
and pt.portfolio_dim_id is null;

update idw.planview_timesheet_entries pt
set date_dim_id = d.sk_date 
from idw.date_dimension d
where pt.entrydate::date = d.dt and pt.date_dim_id is null;
''')

insert_timesheet_entries = ('''insert into idw.planview_timesheet_entries
            (timesheetid, companyid, entrydate, entryhours, entryid, entrytypeid, externalid, internalrate, billablerate, 
            isbillable, isproductive, level1id, level2id, level3id, locationid, notes, state, userid, date_created, is_active) 
            select timesheetid::bigint, companyid::bigint, entrydate, entryhours, entryid::bigint, entrytypeid::bigint, 
            externalid, internalrate, billablerate, isbillable, isproductive, level1id::bigint, level2id::bigint, 
            level3id::bigint, locationid::bigint, notes, state, userid::bigint, now(), true 
            from timesheet_data where entryid is not null''')

update_is_active_flag_for_ts_entries = ('''update idw.planview_timesheet_entries set is_active=false where timesheetid in (select timesheetid from timesheet_data);''')

get_user_list_to_pull_timesheet_entries = ('''
select string_agg(resource_ppm_id::varchar(15), ', ') AS user_list,
to_char(coalesce(max_entryDate,'20200101'::timestamp), 'YYYYMMDD') as min
from 
	(select res.resource_ppm_id, max(entryDate) as max_entryDate
	from (select r.resource_ppm_id,r.resource_dim_id,t.team_dim_id from idw.user_team_map ut
		  inner join idw.team_dim t on ut.team_dim_id=t.team_dim_id                   
		  inner join idw.planview_resource_dim r ON ut.user_dim_id = r.resource_dim_id
		   where is_reportable=true) res
	left join idw.planview_timesheet_entries pt on res.resource_dim_id=pt.resource_dim_id 
	where lower(coalesce(state,'Fully Approved')) = lower('Fully Approved')
	group by res.resource_ppm_id) max_ts_date
group by max_entryDate;''')

remove_system_user_from_team_map = ('''
delete from idw.user_team_map 
where user_dim_id in (select user_dim_id from idw.user_team_map u inner join idw.planview_resource_dim r
on u.user_dim_id=r.resource_dim_id
where type is null);''')


delete_timesheet_entries = ('''select x1.team_dim_id,to_char(x1.from_date, 'YYYY/MM/DD') from_date,to_char(x1.to_date, 'YYYY/MM/DD') to_date,string_agg(x1.resource_ppm_id::varchar(15), ', ') AS resource_list from (
    select a.*,c.resource_ppm_id,c.resource_first_name
    from idw.timesheet_refresh_log a
    inner join (select * from idw.user_team_map where is_active = true) b
    on a.team_dim_id=b.team_dim_id
    inner join  idw.planview_resource_dim c
    on b.user_dim_id = c.resource_dim_id
    where is_timesheet_refreshed=false
    ) x1
    group by team_dim_id,from_date,to_date''')