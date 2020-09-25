from data_extractor_dao import executeQuery, executeQueryAndReturnDF, dbConnection
import planview_acces_methods as planview
import audit_logger as al
import pandas as pd
import json
import zeep
import io
from basic_updates import executeBasicQueries

insertEntitiesQuery = ('''insert into idw.planview_entity_dim  (entity_name, entity_ppm_id, load_frequency, column_select_list, load_sequence)
select 'Portfolio',114,'0 12 ? * SUN *', '{"id": 11401, "title": 11402}'::jsonb,1
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =114) union
select 'Project',4,'0 12 ? * SUN *', '{"id": 435,"ahaid": 2473870467,"title": 436,"status": 411,"category": 400027,"startdate": 428,"targetdate": 430,"canbetemplate": 402,"completiondate": 403,"associatedportfolio": 2476747173}'::jsonb,2
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =4) union
select 'Resources',11,'0 12 ? * SUN *','{"id": 101127,"type": 1126,"unitid": 101113,"emailId": 1115,"type_id": 1112,"lastName": 1104,"firstName": 1103,"full_name": 1136,"hire_date": 1101,"primaryroleID": 1110,"termination_date": 1102}'::jsonb, 5
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =11) union
select 'Unit',229,'0 12 ? * SUN *', '{"id": 22905, "title": 22901, "managerId": 22902, "parentTitle": 22904}'::jsonb, 4
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =229) union
select 'Task',5,'0 12 ? * SUN *', '{"id": 522,"Title": 529,"projectId": 528,"startdate": 518,"targetdate": 520,"TimeReportable": 2490397259,"completiondate": 502,"investmentType": 2471647575, "isRnD": 2471629734, "isCapitalized": 563, "taskType": 513}'::jsonb, 3  where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =5)  union
select 'Events',192,'0 12 ? * SUN *', '{"id": 19201,"title": 19202,"toDate": 19207,"typeId": 19204,"fromDate": 19206,"calendarId": 19203,"workingTime": 19217,"timesheetEntryType": 19209}'::jsonb, 6
where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =192) union
select 'rate_table',280,'0 12 ? * SUN *', '{"active": 28004,"isDefault": 28006,"description": 28003,"id": 28001,"retired": 28005,"title": 28002}'::jsonb,7
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =280) union
select 'rate_user_map',283,'0 12 ? * SUN *', '{"id": 28300,"rateId": 28301,"humanResourceId": 28302,"effectiveDate": 28303}'::jsonb,8
where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =283) union
select 'rate_details',281,'0 12 ? * SUN *', '{"rateId": 28105,"capExRate": 28103,"effectiveDate": 28104,"id": 28101,"opExRate": 28102}'::jsonb,9
 where not exists (select 'x' from idw.planview_entity_dim where entity_ppm_id =281);''')

insert_admin_tasks = ('''insert into idw.planview_admin_tasks (admin_task_name,admin_task_ppm_id)
select 'Holiday' ,7 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=7)
union
select 'Jury Duty' ,5 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=5)
union
select 'PTO' ,4 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=4)
union
select 'Bereavement' ,3 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=3)
union
select 'Floating Holiday' ,2 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=2)
union
select 'Workers Comp' ,103 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=103)
union
select 'Short-term Disability' ,102 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=102)
union
select 'Maternity/Paternity Leave' ,101 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=101)
union
select 'Leave without Pay' ,100 where not exists (select 'x' from idw.planview_admin_tasks where admin_task_ppm_id=100); 
''')

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
    executeQuery(insert_admin_tasks)


def getandUpdateEntitySchema():
    """
    get field details from planview for each entity which is newly loaded into entity dimension

    Args:
    No Argument

    Returns:
    No Return
    """

    dfEntities = executeQueryAndReturnDF('select entity_dim_id, entity_ppm_id from idw.planview_entity_dim where entity_schema is null')
    dfEntities.apply(lambda entity: updateEntitySchema(entity['entity_dim_id'], planview.getEntityFields(int(entity['entity_ppm_id']))), axis=1)


def updateEntitySchema(entity_dim_id, schema):
    """
    update planview schema information for each entity which is newly loaded into entity dimension

    Args:
    No Argument

    Returns:
    No Return
    """
    executeQuery('''update idw.planview_entity_dim set entity_schema='{0}' where entity_dim_id={1};'''.format(schema, entity_dim_id))


def loadEntityData():
    """
    This function initiate data extraction from planview for all major entities like 
    Project, Tasks, Resources, Unit etc.

    Args:
    No Argument

    Returns:
    No Return
    """
    subtaskid = al.createSubTask("Initiate data extraction from planview", al.getMaintaskId())
    planview_entities = executeQueryAndReturnDF("select entity_ppm_id,column_select_list,entity_name from idw.planview_entity_dim order by load_sequence")
    for entity in planview_entities.iterrows():
        # get entity details required to pull data from planview
        ppm_id = int(entity[1]['entity_ppm_id'])
        column_select_list = entity[1]['column_select_list']
        entity_name = entity[1]['entity_name']

        get_list= ",".join([str(e) for e in column_select_list.values()])
        mList = [int(e) if e.isdigit() else e for e in get_list.split(",")]
        json_keys = column_select_list.keys()
        get_Cols = ",".join([str(e) for e in json_keys])
        column_list = [int(e) if e.isdigit() else e for e in get_Cols.split(",")]
        data = planview.getData(ppm_id,mList)
        df = data['methodValues']
        dfObj = process_entity_data(df, column_list)
        temp_table = get_table_schema(column_list, 'pv_entity_temp_table')
        createDf(dfObj, temp_table, entity_name)
        dfObj.drop(columns = column_list, inplace = True)
        df.drop(df.index, inplace = True)

    al.updateSubTask(subtaskid, "SUCCESS")

def get_table_schema(column_list, table_name):
    """
    This function dynamically generate create table SQL, 
    to create temporary table to persist data pulled from planview.

    Args:
    list   : list of columns to create table schema 
    string : name of table to be created with given columns

    Returns:
    String : table create SQL statment to dynamically create temporary table
    """
    tablestring = " create temp table "+table_name+" ("
    for columnname in column_list:
        tablestring = tablestring+'''{0} varchar(100)'''.format(columnname)
        if column_list.index(columnname) < len(column_list)-1:
            tablestring = tablestring+","
    tablestring = tablestring+");"
    return tablestring


def createDf(df, temp_table, entity_name):
    """
    This function insert entity data in temporary table and call function 
    to process data for respective entity using newly created table.

    Args:
    dataframe : take dataframe with source data as input 
    string    : temporary table inwhile data frame will get persisted
    string    : entity name for which data is being loaded and processed

    Returns:
    No Return
    """
    conn = dbConnection()
    cur = conn.cursor()
    cur.execute('''{0}'''.format(temp_table))
    output = io.StringIO()
    df = df.replace('None','')
    df.to_csv(output, sep='|', header=True, index=False)
    output.seek(0)
    copy_query = "COPY pv_entity_temp_table FROM STDOUT csv DELIMITER '|' NULL ''  ESCAPE '\\' HEADER "
    cur.copy_expert(copy_query, output)
    proc_name = ("idw.planview_" + entity_name).lower()
    cur.callproc(proc_name)
    conn.commit()


def process_entity_data(df, column_list):
    """
    This function process raw data received from planview. 
    Planview provide data as ordered dict, 
    this function convert ordered dict in dataframe so that data can be imported to table

    Args:
    dataframe : take dataframe with raw data as input 
    string    : list of columns for whcih data needs to be populated in dataframe

    Returns:
    dataframe : return formatted dataframe with required columns
    """
    subtaskid = al.createSubTask("Create dataframe of planview and temp table string", al.getMaintaskId())
    dfObj = pd.DataFrame()

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
        dfObj = dfObj.append(df1)
        df1.columns = [column_list]
        al.updateSubTask(subtaskid, "SUCCESS")
        return dfObj
        
    except (Exception) as error:
        al.insertErrorLog(subtaskid, error)


def handler(event, context):
    al.createMainTask("Planview entity data extraction", "COGS-Planview-Timesheet")
    insertCommonEntities()
    getandUpdateEntitySchema()
    loadEntityData()
    executeBasicQueries()
    al.updateMainTask(al.getMaintaskId(), "SUCCESS", "Planview entity data extraction")

if __name__ == "__main__":
    #al.createMainTask("Planview entity data extraction", "COGS-Planview-Timesheet")
    insertCommonEntities()
    getandUpdateEntitySchema()
    loadEntityData()
    executeBasicQueries()
    #al.updateMainTask(al.getMaintaskId(), "SUCCESS", "Planview entity data extraction")