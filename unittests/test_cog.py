import unittest
import sys
import os
try:
    from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
    from planview_login import getSession
    import planview_acces_methods as planview
except ModuleNotFoundError:
    sys.path.append(os.path.dirname(
        os.path.dirname(os.path.realpath(__file__))))
    from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
    from planview_login import getSession
    import planview_acces_methods as planview

import requests
import json


class TestCog(unittest.TestCase):

    def test_max_time_spent_by_user(self):
        # check maximum hours spent by any users
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("""select (sum(time_spent)/ actual_sprint_days):: numeric(10) as hr
        from idw.task_time_spent_fact f inner join
        idw.jira_sprint_summary ss
        on f.sprint_number=ss.sprint_number and f.team_dim_id=ss.team_dim_id
        where f.sprint_number=2448 and portfolio_dim_id is not null
        group by f.team_dim_id,f.sprint_number,ss.actual_sprint_days""")
        rows = cur.fetchall()
        summary = round(rows[0][0])
        self.assertEqual(summary, 8)
    
    def test_time_spent_by_team(self):
        # check number of developers
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("""select sum(per_day_time_spent)::numeric(10) as hr
        from idw.planview_get_timesheet_entries('2448')
        group by team_name, sprint_name;""")
        rows = cur.fetchall()
        summary = round(rows[0][0])
        cur.execute("""select (sum(time_spent)* number_of_devs)::numeric(10) as hr
        from idw.task_time_spent_fact f inner join 
        idw.jira_sprint_summary ss 
        on f.sprint_number=ss.sprint_number and f.team_dim_id=ss.team_dim_id
        where f.sprint_number=2448 and portfolio_dim_id is not null
        group by f.team_dim_id,f.sprint_number,ss.number_of_devs;""")
        row1 = cur.fetchall()
        issues = round(row1[0][0])
        self.assertEqual(summary, issues)

    def test_timesheet_generator(self):
        planview_entities = executeQueryAndReturnDF("select string_agg(a1.resource_ppm_id::varchar(15), ', ') AS user_list, to_char(min(a1.sprint_start_date), 'YYYY/MM/DD') as min from ( SELECT d.resource_ppm_id, min(a.sprint_start_date) as sprint_start_date FROM idw.jira_sprint_summary a inner join idw.user_team_map c ON a.team_dim_id = c.team_dim_id inner join idw.planview_resource_dim d ON c.user_dim_id = d.resource_dim_id AND c.team_entry_date <= a.sprint_start_date AND COALESCE(c.team_exit_date::timestamp with time zone, now()) >= a.sprint_end_date where coalesce(a.is_timesheet_populated,false)=false and a.sprint_end_date > '2020-05-14' and a.sprint_end_date <= current_date group by d.resource_ppm_id ) a1")
        users = planview_entities['user_list']
        self.assertTrue(users[0])

    def test_planview_login(self):
        client, sessionId  = getSession()
        self.assertTrue(sessionId)

    def test_fetch_timesheet_entries(self):
        planview_entities = executeQueryAndReturnDF("select string_agg(res.resource_ppm_id::varchar(15), ', ') AS user_list,'2020/01/01' as min from (select r.resource_ppm_id,r.resource_dim_id from idw.user_team_map ut inner join idw.team_dim t on ut.team_dim_id=t.team_dim_id inner join idw.planview_resource_dim r ON ut.user_dim_id = r.resource_dim_id where is_reportable=true) res")
        conn = dbConnection()
        cur = conn.cursor()
        for index, row in planview_entities.iterrows():
            users = row['user_list']
            if users is not None: 
                mList = [int(e) if e.isdigit() else int(e) for e in users.split(',')]
            else:
                raise Exception("User list is Empty")
            startDate = row['min']
            sheet = planview.getTimesheet_users(mList,startDate)
            self.assertTrue(len(sheet.index))
            break
    

if __name__ == '__main__':
    print('starting unittest')
    unittest.main()
    print('Unittest complete')
