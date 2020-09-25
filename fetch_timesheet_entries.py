from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
import db_queries as dq
import planview_acces_methods as planview
import pandas as pd
import audit_logger as al
import io
from send_notification import send_custom_mail


def timesheet_entry_all_user():
    """
    This function pull timesheet entries from planview for users and insert into planview_timesheet_entries table 

    Args:
    No Arguments

    Returns:
    No Return
    """
    subtaskid = al.createSubTask(
        "Pull timesheet entries from planview for users and insert into planview_timesheet_entries table",
        al.getMaintaskId())
    try:
        #Fetch user ids for processing
        planview_entities = executeQueryAndReturnDF(dq.get_user_list_to_pull_timesheet_entries)
        conn = dbConnection()
        cur = conn.cursor()
        for index, row in planview_entities.iterrows():
            users = row['user_list']
            if users is not None: 
                mList = [int(e) if e.isdigit() else int(e) for e in users.split(',')]
            else:
                raise Exception("User list is Empty")
            startDate = row['min']

            #Fetch timesheet entries for user list
            sheet = planview.getTimesheet_users(mList,startDate)

            column_names = ['timesheetId', 'companyId', 'entryDate', 'entryHours', 'entryId', 'entryTypeId', 'externalId', 'internalRate', 'billableRate', 'isBillable', 'isProductive', 'level1Id', 'level2Id', 'level3Id', 'locationId', 'notes', 'state', 'userId']
            user_timesheet_data = pd.DataFrame(columns=column_names)

            for tsrow in sheet.itertuples(index=True, name='Pandas'):
            
                for entry in tsrow.entriesProject:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'companyId': entry['companyId'], 'entryDate': entry['entryDate'],'entryHours': entry['entryHours'],'entryId': entry['entryId'],'entryTypeId': entry['entryTypeId'] ,'externalId': entry['externalId'] ,'internalRate': entry['internalRate'] ,'billableRate': entry['billableRate'] ,'isBillable': entry['isBillable'] ,'isProductive': entry['isProductive'] ,'level1Id': entry['level1Id'],'level2Id': entry['level2Id'],'level3Id': entry['level3Id'],'locationId': entry['locationId'],'notes': entry['notes'],'state': entry['state'],'userId':tsrow.userId}, ignore_index=True)
                for entry1 in tsrow.entriesOther:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'companyId': entry1['companyId'], 'entryDate': entry1['entryDate'],'entryHours': entry1['entryHours'],'entryId': entry1['entryId'],'entryTypeId': entry1['entryTypeId'] ,'externalId': entry1['externalId'] ,'internalRate': entry1['internalRate'] ,'billableRate': entry1['billableRate'] ,'isBillable': entry1['isBillable'] ,'isProductive': entry1['isProductive'] ,'level1Id': entry1['level1Id'],'level2Id': entry1['level2Id'],'level3Id': entry1['level3Id'],'locationId': entry1['locationId'],'notes': entry1['notes'],'state': entry1['state'],'userId':tsrow.userId}, ignore_index=True)
                for entry2 in tsrow.entriesAccount:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'companyId': entry2['companyId'], 'entryDate': entry2['entryDate'],'entryHours': entry2['entryHours'],'entryId': entry2['entryId'],'entryTypeId': entry2['entryTypeId'] ,'externalId': entry2['externalId'] ,'internalRate': entry2['internalRate'] ,'billableRate': entry2['billableRate'] ,'isBillable': entry2['isBillable'] ,'isProductive': entry2['isProductive'] ,'level1Id': entry2['level1Id'],'level2Id': entry2['level2Id'],'level3Id': entry2['level3Id'],'locationId': entry2['locationId'],'notes': entry2['notes'],'state': entry2['state'],'userId':tsrow.userId}, ignore_index=True)
                for entry3 in tsrow.entriesPortfolio:
                    user_timesheet_data = user_timesheet_data.append({'timesheetId': tsrow.timesheetId,'companyId': entry3['companyId'], 'entryDate': entry3['entryDate'],'entryHours': entry3['entryHours'],'entryId': entry3['entryId'],'entryTypeId': entry3['entryTypeId'] ,'externalId': entry3['externalId'] ,'internalRate': entry3['internalRate'] ,'billableRate': entry3['billableRate'] ,'isBillable': entry3['isBillable'] ,'isProductive': entry3['isProductive'] ,'level1Id': entry3['level1Id'],'level2Id': entry3['level2Id'],'level3Id': entry3['level3Id'],'locationId': entry3['locationId'],'notes': entry3['notes'],'state': entry3['state'],'userId':tsrow.userId}, ignore_index=True)
            
                user_timesheet_data['isBillable'] = user_timesheet_data['isBillable'].astype('bool')
                user_timesheet_data['isProductive'] = user_timesheet_data['isProductive'].astype('bool')

            #export timesheet details into temporary table
            cur.execute('''drop table if exists timesheet_data;''')
            cur.execute('''create temp table timesheet_data (timesheetid numeric, companyid numeric, entrydate date, entryhours numeric, entryid numeric, entrytypeid numeric, externalid varchar(100), internalrate numeric, billablerate numeric, isbillable boolean, isproductive boolean, level1id numeric, level2id numeric, level3id numeric, locationid numeric, notes text, state text, userid numeric);''')
            output = io.StringIO()
            user_timesheet_data.to_csv(output, sep='~', header=True, index=False)
            output.seek(0)
            copy_query = "COPY timesheet_data FROM STDOUT csv DELIMITER '~' NULL ''  ESCAPE '\\' HEADER "
            cur.copy_expert(copy_query, output)
            conn.commit()
            cur.execute(dq.update_is_active_flag_for_ts_entries)
            cur.execute(dq.insert_timesheet_entries)
            cur.execute(dq.update_timesheet_dim_ids)
            conn.commit()
        al.updateSubTask(subtaskid, "SUCCESS")
    except Exception as error:
        print(error)
        send_custom_mail("Failed to fetch timesheet entries from planview", error)
        al.insertErrorLog(subtaskid, error)


def handler(event, context):
    al.createMainTask("TimeSheet entries Extraction", "COGS-Planview-Timesheet")
    timesheet_entry_all_user()
    al.updateMainTask(al.getMaintaskId(), "SUCCESS", "TimeSheet entries Extraction")
