from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
from planview_acces_methods import getEntityFields, getData , getTimesheet, getTimesheet_users ,insertTimesheet,insertTimesheet1
from audit_logger import createSubTask, getMaintaskId, insertErrorLog, updateSubTask
import pandas as pd
import json
import zeep
import io

insertEntitiesQuery = ('''insert into idw.planview_entity_dim  (entity_name, entity_ppm_id, load_frequency, column_select_list, load_sequence)
select 'Portfolio',114,'0 12 ? * SUN *', '{"id": 11401, "title": 11402}'::jsonb,1
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =114) union
select 'Project',4,'0 12 ? * SUN *', '{"id": 435,"ahaid": 2473870467,"title": 436,"status": 411,"category": 400027,"startdate": 428,"targetdate": 430,"canbetemplate": 402,"completiondate": 403,"associatedportfolio": 2476747173}'::jsonb,2
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =4) union
select 'Resources',11,'0 12 ? * SUN *','{"id": 101127,"type": 1126,"unitid": 101113,"emailId": 1115,"type_id": 1112,"lastName": 1104,"firstName": 1103,"full_name": 1136,"hire_date": 1101,"primaryroleID": 1110,"termination_date": 1102}'::jsonb, 5
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =11) union
select 'Unit',229,'0 12 ? * SUN *', '{"id": 22905,"title": 22901,"managerId": 22902}'::jsonb, 4
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =229) union
select 'Task',5,'0 12 ? * SUN *', '{"id": 522,"Title": 529,"projectId": 528,"startdate": 518,"targetdate": 520,"TimeReportable": 2490397259,"completiondate": 502}'::jsonb, 3  where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =5)  union
select 'Events',192,'0 12 ? * SUN *', '{"id": 19201,"title": 19202,"toDate": 19207,"typeId": 19204,"fromDate": 19206,"calendarId": 19203,"workingTime": 19217,"timesheetEntryType": 19209}'::jsonb, 6
where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =192);

;''')


def insertCommonEntities():
    """
    static insert all required entities in entity dimension.
    this is one time insert

    Args:
    No Argument

    Returns:
    No Return
    """
    executeQuery(insertEntitiesQuery)

def getandUpdateEntitySchema():
    """
    get field details from planview for each entity which is newly loaded into entity dimension

    Args:
    No Argument

    Returns:
    No Return
    """

    dfEntities = executeQueryAndReturnDF('select entity_dim_id, entity_ppm_id from idw.planview_entity_dim where entity_schema is null')
    dfEntities.apply(lambda entity: updateEntitySchema(entity['entity_dim_id'], getEntityFields(int(entity['entity_ppm_id']))), axis=1)

def updateEntitySchema(entity_dim_id, schema):
    """
    update planview schema information for each entity which is newly loaded into entity dimension

    Args:
    No Argument

    Returns:
    No Return
    """
    executeQuery('''update idw.planview_entity_dim set entity_schema='{0}' where entity_dim_id={1};'''.format(schema, entity_dim_id))

dfObj = pd.DataFrame()
temp_table=''

def loadEntityData():
    subtaskid = createSubTask("Load planview data into tables", getMaintaskId())
    try:
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute("select entity_ppm_id from idw.planview_entity_dim order by load_sequence")
        rows1 = cur.fetchall()
       # rows1 = [5]
        for row in rows1:
            ppm_id = row[0]
            #ppm_id=row
            cur.execute("select column_select_list from idw.planview_entity_dim where entity_ppm_id={0}".format(ppm_id))
            rows = cur.fetchall()
            json_dict = rows[0][0].values()
            get_list= ",".join([str(e) for e in json_dict])
            mList = [int(e) if e.isdigit() else e for e in get_list.split(",")]
            json_keys = rows[0][0].keys()
            get_Cols = ",".join([str(e) for e in json_keys])
            column_list = [int(e) if e.isdigit() else e for e in get_Cols.split(",")]
            data = getData(ppm_id,mList)
            df = data['methodValues']
            print(df)
            test_df(df,column_list)
            ##df.apply(lambda row: getPlanviewData(row, column_list, ppm_id))
            createDf(ppm_id, dfObj, temp_table)
            print(dfObj)
            dfObj.drop(columns= column_list, inplace=True)
            df.drop(df.index, inplace=True)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)
    
    subtaskid = createSubTask("Load planview data into tables", getMaintaskId())
    try:
        
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)

def createDf(ppm_id, df, temp_table):
    subtaskid = createSubTask("Create temp table to load the planview data into db tables", getMaintaskId())
    try:
        conn = dbConnection()
        cur = conn.cursor()
        cur.execute('''{0}'''.format(temp_table))
        output = io.StringIO()
        df.to_csv(output, sep='|', header=True, index=False)
        output.seek(0)
        copy_query = "COPY temp_table FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
        cur.copy_expert(copy_query, output)
        cur.execute('''select entity_name from idw.planview_entity_dim where entity_ppm_id = {0}'''.format(ppm_id))
        ppm = cur.fetchall()
        proc_name = ("idw.proc_" + ppm[0][0]).lower()
        cur.callproc(proc_name)
        conn.commit()
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)

def getPlanviewData(mylist, column_list, ppm_id):
    subtaskid = createSubTask("Create dataframe of planview and temp table string", getMaintaskId())
    try:
        entityname = 'temp_table'
        strmy="{"
        tablestring = " create temp table "+entityname+" ("
        for columnname in column_list:
            strmy = strmy+'''"{0}": "{1}"'''.format(columnname, str(mylist[column_list.index(columnname)]['elementValue']).replace("\"", "").replace("\\","\\\\"))
            tablestring = tablestring+'''{0} varchar(100)'''.format(columnname)
            if column_list.index(columnname) < len(column_list)-1:
                strmy = strmy+','
                tablestring = tablestring+","
        strmy = strmy+'}'
        global temp_table
        tablestring = tablestring+");"
        temp_table = tablestring
        global dfObj
        dfObj = dfObj.append(dict(json.loads(strmy)), ignore_index=True)
        dfObj = dfObj[column_list]
        dfObj.fillna(value='NULL', inplace=True)
        updateSubTask(subtaskid, "SUCCESS")
    except (Exception) as error:
        insertErrorLog(subtaskid, error)

def test_df(df,column_list):
    subtaskid = createSubTask("Create dataframe of planview and temp table string", getMaintaskId())
    try:
        val = []
        val_list = []
        for i in df: 
            val = []
            for v in i:
                x = list(v.values())[0]
                val.append(x)
            val_list.append(val)
        df1 = pd.DataFrame(val_list)
        df1.columns = [column_list]
        global dfObj
        dfObj = dfObj.append(df1)
        df1.columns = [column_list]
        entityname = 'temp_table'
        tablestring = " create temp table "+entityname+" ("
        for columnname in column_list:
            tablestring = tablestring+'''{0} varchar(100)'''.format(columnname)
            if column_list.index(columnname) < len(column_list)-1:
                tablestring = tablestring+","
        global temp_table
        tablestring = tablestring+");"
        temp_table = tablestring
    except (Exception) as error:
        insertErrorLog(subtaskid, error)

def timesheet_entry():
    sheet = getTimesheet(2506182696,'2020/01/01','2020/12/31')
    print(sheet['timesheetId'])
    print(sheet.to_string())

def timesheet_entry_user():

    #Fetch user ids for processing
    planview_entities = executeQueryAndReturnDF("select string_agg(a1.resource_ppm_id::varchar(15), ', ') AS user_list, to_char(min(a1.sprint_start_date), 'YYYY/MM/DD') as min from ( SELECT d.resource_ppm_id, min(a.sprint_start_date) as sprint_start_date FROM idw.jira_sprint_summary a inner join idw.user_team_map c ON a.team_dim_id = c.team_dim_id inner join idw.planview_resource_dim d ON c.user_dim_id = d.resource_dim_id AND c.team_entry_date < a.sprint_start_date AND COALESCE(c.team_exit_date::timestamp with time zone, now()) > a.sprint_end_date where coalesce(a.is_timehseet_populated,false)=false and a.sprint_end_date > '2020-05-14' and a.sprint_end_date <= current_date group by d.resource_ppm_id ) a1")
    users = planview_entities['user_list']
    mList = [int(e) if e.isdigit() else int(e) for e in users[0].split(',')]
    startDate = planview_entities['min']
    #Fetch timesheet ids for above user list
    sheet = getTimesheet_users(mList,startDate[0])
    #print(row['entryHours'],row['entryDate'],row['timesheetId'])
  
    timesheet_data = sheet[['timesheetId','userId','startDate','endDate']]
    #print(timesheet_data)
    conn = dbConnection()
    cur = conn.cursor()
    cur.execute('''create temp table timesheet_entry (entryId bigint, timesheetId bigint, entryDate date, entryHours numeric) ''')
    l = sheet['entriesProject']
    for row in l:
        for x in row:
            cur.execute('''insert into timesheet_entry(entryId,timesheetId,entryDate,entryHours) values ({0},{1},'{2}','{3}')'''.format(x['entryId'],x['timesheetId'],x['entryDate'],x['entryHours']))
            conn.commit()

    #Creating temporary table to store timesheet data
    cur.execute('''create temp table timesheet_data (timesheetId bigint, userId bigint, startDate date, endDate date) ''')
    output = io.StringIO()
    timesheet_data.to_csv(output, sep='|', header=True, index=False)
    output.seek(0)
    copy_query = "COPY timesheet_data FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
    cur.copy_expert(copy_query, output)
    conn.commit()
    #Fetch list of sprint numbers
    sprint_number = executeQueryAndReturnDF("SELECT string_agg(sprint_number::varchar(15), ', ') AS sprint_number from (select distinct sprint_number FROM idw.jira_sprint_summary a where coalesce(a.is_timehseet_populated,false)=false and a.sprint_end_date > '2020-05-14' and a.sprint_end_date <= current_date)x1")
    sprint_list = sprint_number['sprint_number'][0]
    print(sprint_list)

    #Joining timesheet data and sprint data
    #query = '''select pd.*, td.timesheetId::bigint timesheetId, td.entryid, td.entrydate from (select  resource_ppm_id::bigint,to_char(date_of_sprint, 'YYYY/MM/DD') date_of_sprint,per_day_time_spent,project_ppm_id::bigint,task_ppm_id::bigint,resource_role_id::bigint from idw.planview_get_timesheet_entries('2448, 2506, 2519, 2552')) pd  inner join (select x1.timesheetid,x2.entryid, x2.entrydate, x1.userId, x1.startDate, x1.endDate 			from timesheet_data x1 			left outer join timesheet_entry x2 			on x1.timesheetid = x2.timesheetid) td  on pd.resource_ppm_id = td.userId  and case when entrydate is null then (date_of_sprint::date >= startDate::date and date_of_sprint::date <= endDate::date) else (date_of_sprint::date = entrydate::date) end'''.format(sprint_list)
    #timesheet_entry =  pd.read_sql_query(query, con=conn)

    entryTypeId = 1
    companyId = 1
    query1 = '''select timesheetid,userid from timesheet_data'''
    timesheet_users =  pd.read_sql_query(query1, con=conn)
    print(timesheet_users)
    for index, row1 in timesheet_users.iterrows():
        print(row1['timesheetid'],row1['userid'])
        #query = '''select pd.*, td.timesheetId::bigint timesheetId, td.entryid, td.entrydate from (select  resource_ppm_id::bigint,to_char(date_of_sprint, 'YYYY/MM/DD') date_of_sprint,per_day_time_spent,project_ppm_id::bigint,task_ppm_id::bigint,resource_role_id::bigint from idw.planview_get_timesheet_entries('2448, 2506, 2519, 2552')) pd  inner join (select x1.timesheetid,x2.entryid, x2.entrydate, x1.userId, x1.startDate, x1.endDate 			from timesheet_data x1 			left outer join timesheet_entry x2 			on x1.timesheetid = x2.timesheetid) td  on pd.resource_ppm_id = td.userId  and case when entrydate is null then (date_of_sprint::date >= startDate::date and date_of_sprint::date <= endDate::date) else (date_of_sprint::date = entrydate::date) end  where pd.resource_ppm_id = '{1}' and td.timesheetid = {2} '''.format(sprint_list, row['timesheetid'], row['userid'])
        query = '''select pd.*, td.timesheetId::bigint timesheetId, td.entryid, to_char(td.entrydate::date, 'YYYY/MM/DD') entrydate from (select  resource_ppm_id::bigint,to_char(date_of_sprint, 'YYYY/MM/DD') date_of_sprint,per_day_time_spent,project_ppm_id::bigint,task_ppm_id::bigint,resource_role_id::bigint from idw.planview_get_timesheet_entries('2448, 2506, 2519, 2552')) pd  inner join (select x1.timesheetid,x2.entryid, x2.entrydate, x1.userId, x1.startDate, x1.endDate 			from timesheet_data x1 			left outer join timesheet_entry x2 			on x1.timesheetid = x2.timesheetid) td  on pd.resource_ppm_id = td.userId  and case when entrydate is null then (date_of_sprint::date >= startDate::date and date_of_sprint::date <= endDate::date) else (date_of_sprint::date = entrydate::date) end  where pd.resource_ppm_id = {1} and td.timesheetid = {2} '''.format(sprint_list, row1['userid'], row1['timesheetid'])
        timesheet_entry =  pd.read_sql_query(query, con=conn)
        print(timesheet_entry)
        strmy=[]
        str1 = '['
        for index, row in timesheet_entry.iterrows():
            print(row)
            l1= row['entryid']
            print(l1)
            #x = "{" + '''"companyId": 1, "entryId" : {0}, "entryDate" :  '{1}', "entryHours": {2},"entryTypeId": 1, "level1Id": {3}, "level2Id": {4}, "level3Id": {5}, "timesheetId": {6}'''.format(row['entryid'],row['entrydate'],row['per_day_time_spent'],row['project_ppm_id'],row['task_ppm_id'],row['resource_role_id'],row['timesheetid']) + "}"
            x = '{' + """'companyId': 1, 'entryId' : {0}, 'entryDate' :  '{1}', 'entryHours': {2},'entryTypeId': 1, 'level1Id': {3}, 'level2Id': {4}, 'level3Id': {5}, 'timesheetId': {6}""".format(row['entryid'],row['entrydate'],row['per_day_time_spent'],row['project_ppm_id'],row['task_ppm_id'],row['resource_role_id'],row['timesheetid']) + '}'
            #strmy.append(x)
            str1 = str1 + x + ","
        x1 = str1.rstrip(',') + "]"
        print(x1)
        if x1 != '[]' :
            insertTimesheet1(row1['timesheetid'],row1['userid'], x1)
        

        
if __name__ == "__main__":
    #insertCommonEntities()
    #getandUpdateEntitySchema()
    #loadEntityData()
    #timesheet_entry()
    timesheet_entry_user()