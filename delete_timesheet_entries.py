from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection, dbEngine
import planview_acces_methods as planview
import pandas as pd
import db_queries as dq
import audit_logger as al
import io

def delete_entries():
    subtaskid = al.createSubTask("Delete timesheet entries for a team", al.getMaintaskId())
    try:
        timesheet_team = executeQueryAndReturnDF(dq.delete_timesheet_entries)

        for team in timesheet_team.itertuples(index=True, name='Pandas'):
            print(team.resource_list)
            users = team.resource_list
            if users is not None:    
                mList = [int(e) if e.isdigit() else int(e) for e in users.split(',')]
            
            team_id = team.team_dim_id
            startDate = team.from_date
            endDate = team.to_date
            #Fetch timesheet ids for above user list
            sheet = planview.getTimesheet_users(mList,startDate)
            
            column_names = ['timesheetId','userId','startDate','endDate','entryId','entryDate','entryHours','project_ppm_id','task_ppm_id']

            user_timesheet_data = pd.DataFrame(columns=column_names)

            for tsrow in sheet.itertuples(index=True, name='Pandas'):
                if not tsrow.entriesProject:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'userId':tsrow.userId, 'startDate':tsrow.startDate, 'endDate':tsrow.endDate,'entryId': None,'entryDate': None,'entryHours': None,'project_ppm_id': None,'task_ppm_id': None}, ignore_index=True)
                else:
                    for entry in tsrow.entriesProject:
                        user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId, 'userId':tsrow.userId, 'startDate':tsrow.startDate, 'endDate':tsrow.endDate,'entryId': entry['entryId'],'entryDate': entry['entryDate'],'entryHours': entry['entryHours'],'project_ppm_id': entry['level1Id'],'task_ppm_id': entry['level2Id']}, ignore_index=True)
            
            print(user_timesheet_data.to_string())
            #export timesheet details into temporary table
            conn = dbConnection()
            cur = conn.cursor()
            cur.execute('''create temp table timesheet_data_delete (timesheetId bigint, 
            userId bigint, startDate date, endDate date, entryId bigint, 
            entryDate date,entryHours numeric, project_ppm_id bigint, task_id bigint) ''')
            output = io.StringIO()
            user_timesheet_data.to_csv(output, sep='|', header=True, index=False)
            output.seek(0)
            copy_query = "COPY timesheet_data_delete FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
            cur.copy_expert(copy_query, output)
            query = '''select timesheetid,userid,string_agg(entryid::varchar(15), ', ') AS entryid from timesheet_data_delete where entrydate between '{0}' and '{1}' group by timesheetid,userid'''.format(startDate,endDate)
            entries = pd.read_sql_query(query, con=conn)
            print(entries)
            conn.commit()

            for i in entries.itertuples(index=True, name='Pandas'):
                print(i.timesheetid)
                print(i.entryid)
                entries = i.entryid
                if entries is not None:    
                    entry_List = [int(e) if e.isdigit() else int(e) for e in entries.split(',')]
                
                print(entry_List)
                
                timesheet_id = i.timesheetid
                #delete timesheet entries for timesheetid
                planview.deleteTimesheet_entries(timesheet_id,entry_List)
            cur.execute('''update idw.timesheet_refresh_log set is_timesheet_refreshed = true where team_dim_id = {0}'''.format(team_id))
            conn.commit()

    except (Exception) as error:
        al.insertErrorLog(subtaskid, error)
        

if __name__ == "__main__":
    delete_entries()