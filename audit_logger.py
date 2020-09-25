import psycopg2
from data_extractor_dao import dbConnection, executeQuery, executeQueryReturnId, executeQueryAndReturnDF
from send_notification import send_completion_mail, send_start_mail

maintaskid = 0


def getMaintaskId():
    """
     Getter method for main task id,
     this id is to track tasks related to maintask
     i.e. current execution cycle ,

    Args:
    No Arguments
    Returns:
    returns integer, maintaskid is returned.
    This is set as global variable
    when main task entry is inserted in etl_audit table
    """
    return maintaskid


def createMainTask(taskname, product_name):
    """
    Method to create data extarction start entry

    Args:
    String : accpets task name for which audit entry needs to be created
    Returns:
    No return variable
    """
    send_start_mail(taskname)

    maintaskinsertquery = (
        '''insert into birepusr.etl_audit (product_name,task_name,start_date,status)
    values('{product_name}','{tname}',now(),'IN-PROGRESS') RETURNING audit_id''').format(product_name=product_name, tname=taskname)
    global maintaskid
    maintaskid = executeQueryReturnId(maintaskinsertquery)
    print('''Main task started : {0}, (task-id:{1})'''.format(taskname, maintaskid))


def updateMainTask(taskid, status, taskname):
    """
    Method to update data extraction start entry whcih corresponds to main task

    Args:
    1. integer : accpets task id as first argument for which audit entry needs to be updated
    2. string  : accepts staus as second argument for respective task id which needs to be updated

    Returns:
    No return variable
    """
    maintaskupdatequery = ('''update birepusr.etl_audit
    set end_date=now(),
    status='{status}'
    where audit_id={taskid} and end_date IS NULL''').format(status=status, taskid=taskid)
    executeQuery(maintaskupdatequery)
    print('''Main task Ended with status {0},Maintaskid : {1}'''.format(status, taskid))
    if status == "SUCCESS":
        send_completion_mail(None, status, taskname)


def createSubTask(subtaskname, maintaskid):
    """
    Method to create start entry for sub task

    Args:
    1. string  : accpets subtask name as first argument for which audit entry needs to be created
    2. integer : accepts maintask id as second argument for respective sub task which needs to be created.
    this will allow to track workflow and each step whise status for etl

    Returns:
    returns subtask id generated by records insert operation
    """
    subtaskinsertquery = (
        '''insert into birepusr.etl_audit_details (audit_id,sub_task_name,start_date,status)
    values({audit_id},'{tname}',now(),'IN-PROGRESS') RETURNING audit_details_id''').format(audit_id=maintaskid, tname=subtaskname)
    subtaskid = executeQueryReturnId(subtaskinsertquery)
    print('''Subtask started :{0} (subtask-id:{1})'''.format(subtaskname, subtaskid))
    return subtaskid


def updateSubTask(subtaskid, status):
    """
    Method to update data extraction start entry whcih corresponds to respective sub task

    Args:
    1. integer : accpets subtask id as first argument for which audit entry needs to be updated
    2. string  : accepts staus as second argument for respective subtask id which needs to be updated

    Returns:
    No return variable
    """
    subtaskupdatequery = ('''update birepusr.etl_audit_details
    set end_date=now(),
    status='{status}'
    where audit_details_id={taskid}''').format(status=status, taskid=subtaskid)
    print('''Subtask Ended with status {0}, taskid :{1}'''.format(status, subtaskid))
    executeQuery(subtaskupdatequery)


def insertErrorLog(subtaskid, erroMessage):
    """
    Method to create error log entry for sub task

    Args:
    1. integer : accpets subtask name as first argument for which error log entry needs to be created
    2. String  : accepts error as second argument for respective sub task which needs to be created.
    this will allow to track all the errors occured during data extraction

    Returns:
    No return variable. it raise exception which is logged
    """
    errorLoginsertquery = ('''insert into birepusr.etl_error_log (audit_details_id,error_log,create_date)
    values({audit_details_id},'{errormessage}',now()) RETURNING error_log_id''').format(
        audit_details_id=subtaskid, errormessage=str(erroMessage).replace('\'', ''))
    executeQueryReturnId(errorLoginsertquery)
    subtask_name = executeQueryAndReturnDF('''select sub_task_name from birepusr.etl_audit_details where audit_details_id={0};'''.format(subtaskid))
    taskName = subtask_name.loc[0][0]
    updateSubTask(subtaskid, "FAILED")
    updateMainTask(maintaskid, "FAILED",'Planview Data Extraction')
    print('''Exception thrown by Planview ETL : {0}'''.format(erroMessage))
    send_completion_mail(erroMessage, "FAILED", taskName)
    raise Exception(erroMessage)