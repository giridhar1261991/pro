import update_metrics as metrics
from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
import db_queries as dq
import planview_acces_methods as planview
import pandas as pd
import audit_logger as al
import io
import json
from send_notification import send_custom_mail
from variables import environment

def timesheet_entry_user():
    """
    This function process generates data using timesheet and sprint data, 
    to insert timesheet entry into planview. 

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = al.createSubTask(
        "Populate timesheet for latest sprints in planview",
        al.getMaintaskId())
    try:
        #Fetch user ids for processing
        planview_entities = executeQueryAndReturnDF(dq.get_userlist_to_populate_timesheet)
        users = planview_entities['user_list']
        ts_columns = {'companyid':'companyId','entrytypeid':'entryTypeId','timesheetid':'timesheetId','userid':'userId', 'level1id':'level1Id','level2id':'level2Id','entrydate':'entryDate','entryid':'entryId','level3id':'level3Id','entryhours':'entryHours'}

        if users[0] is not None:    
            mList = [int(e) if e.isdigit() else int(e) for e in users[0].split(',')]
        
            startDate = planview_entities['min']
            #Fetch timesheet ids for above user list
            sheet = planview.getTimesheet_users(mList,startDate[0])

            column_names = ['timesheetId','userId','startDate','endDate','entryId','entryDate','entryHours','project_ppm_id','task_ppm_id']

            user_timesheet_data = pd.DataFrame(columns=column_names)

            for tsrow in sheet.itertuples(index=True, name='Pandas'):
                if not tsrow.entriesProject:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'userId':tsrow.userId, 'startDate':tsrow.startDate, 'endDate':tsrow.endDate,'entryId': None,'entryDate': None,'entryHours': None,'project_ppm_id': None,'task_ppm_id': None}, ignore_index=True)
                else:
                    for entry in tsrow.entriesProject:
                        user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId, 'userId':tsrow.userId, 'startDate':tsrow.startDate, 'endDate':tsrow.endDate,'entryId': entry['entryId'],'entryDate': entry['entryDate'],'entryHours': entry['entryHours'],'project_ppm_id': entry['level1Id'],'task_ppm_id': entry['level2Id']}, ignore_index=True)
            
            sprint_number = executeQueryAndReturnDF(dq.get_sprints_for_timesheet)
            sprint_list = sprint_number['sprint_number'][0]
            conn = dbConnection()
            cur = conn.cursor()
            #export timesheet details into temporary table
            cur.execute('''create temp table timesheet_data (timesheetId bigint, 
            userId bigint, startDate date, endDate date, entryId bigint, 
            entryDate date,entryHours numeric, project_ppm_id bigint, task_id bigint) ''')
            output = io.StringIO()
            user_timesheet_data.to_csv(output, sep='|', header=True, index=False)
            output.seek(0)
            copy_query = "COPY timesheet_data FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
            cur.copy_expert(copy_query, output)
            
            #Joining timesheet data and sprint data
            query = dq.timesheet_population_query.format(sprint_list)

            timesheet_entry =  pd.read_sql_query(query, coerce_float=False, con=conn)

            conn.commit()
            timesheet_entry.rename(columns=ts_columns,inplace=True)
            init_timesheet_insert(timesheet_entry,'New')
            cur.execute('''update idw.jira_sprint_summary set is_timesheet_populated = true where sprint_number in ({0})'''.format(sprint_list))
            conn.commit()
        else:
            print("User list is Empty")

        #Processing failed timesheet entries
        failed_entries_df = executeQueryAndReturnDF("select companyId,entryTypeId,timesheetId,userId,level1Id,level2Id,to_char(entryDate,'YYYY/MM/DD') entryDate,entryId,level3Id,entryHours from idw.planview_failed_timesheetID")
        failed_entries_df.rename(columns=ts_columns,inplace=True)
        init_timesheet_insert(failed_entries_df,'Existing')
        al.updateSubTask(subtaskid, "SUCCESS")
    except Exception as error:
        al.insertErrorLog(subtaskid, error)

def init_timesheet_insert(timesheet_entry,entry_flag):
    """
    This function initiate Planview WS API call to insert timesheet entry for given user. 

    Args:
    DataFrame: pandas dataframe with timehseet details for all users for latest sprints
    String: Identifier to distingish fresh timehseet entry or retry attempt of failed timesheet

    Returns:
    No Return
    """
    if not timesheet_entry.empty:
        df_ts = timesheet_entry.groupby(by=['timesheetId','userId'], sort=False).apply(lambda x: x.to_dict(orient='records'))
        df_ts = df_ts.drop(columns="userId")
        for row in df_ts.to_list():
            user_ts_list = {'timesheetId':row[0]['timesheetId'], 'userId': int(row[0]['userId'])}
            user = int(row[0]['userId'])
            timesheet = row[0]['timesheetId']
            for r in row: del r['userId']
            user_ts_list['entriesProject']=row
            try:
                if entry_flag == 'New':
                    planview.insertTimesheet(user_ts_list)
                else:
                    #TO DO optimize this code to perform bul operation at end
                    planview.insertTimesheet(user_ts_list)
                    conn = dbConnection()
                    cur = conn.cursor()
                    cur.execute('''delete from idw.planview_failed_timesheetID where timesheetId = {0} and userId = {1}'''.format(timesheet,user))
                    conn.commit()
            except Exception as e:
                #TO DO: add valid timesheet record and user details in email body
                send_custom_mail("Failed to populate timesheet in planview", e)
                x1 = timesheet_entry[(timesheet_entry.timesheetId == timesheet) & (timesheet_entry.userId == user)]
                insertFailedEntries(x1)


def insertFailedEntries(entry):
    """
    This function insert failed timesheets in idw.planview_failed_timesheetID table 
    so that we can retry them at later point

    Args:
    Dataframe: Dataframe with timesheet object which failed while populating data in planview

    Returns:
    No Return
    """
    conn = dbConnection()
    cur = conn.cursor()
    cur.execute('''create temp table timesheet_entry (companyId  int,entryTypeId int,timesheetId bigint,userId bigint,level1Id bigint,level2Id bigint,entryDate date,entryId bigint,level3Id bigint,entryHours numeric) ''')
    output = io.StringIO()
    entry.to_csv(output, sep='|', header=True, index=False)
    output.seek(0)
    copy_query = "COPY timesheet_entry FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
    cur.copy_expert(copy_query, output)
    cur.execute('''insert into idw.planview_failed_timesheetID(companyId,entryTypeId,timesheetId,userId,level1Id,level2Id,entryDate,entryId,level3Id,entryHours,created_date) select companyId,entryTypeId,timesheetId,userId,level1Id,level2Id,entryDate,entryId,level3Id,COALESCE(entryHours, 0) entryHours,now() as created_date from timesheet_entry where concat(timesheetid,userid) not in (select concat(timesheetid,userid) from idw.planview_failed_timesheetID)''')
    conn.commit()

def handler(event, context):
    al.createMainTask("TimeSheet calculation", "COGS-Planview-Timesheet")
    metrics.initUpdateSprintMetrics()
    metrics.generateTimeSpentMetrics()
    executeQuery(dq.update_maintenance_flag)
<<<<<<< HEAD
    timesheet_entry_user()
    al.updateMainTask(al.getMaintaskId(), "SUCCESS", "TimeSheet calculation")
=======
    if environment == 'staging':
        timesheet_entry_user()
    al.updateMainTask(al.getMaintaskId(), "SUCCESS", "TimeSheet calculation")

if __name__ == "__main__":
    timesheet_entry_user()
>>>>>>> 20934ef144921196d2228a62fcec2177d52ef320
